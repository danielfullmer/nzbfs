import ConfigParser
import errno
import logging
import os
import stat
import subprocess
import threading
import weakref

import xattr

from nzbfs import fuse
from nzbfs.downloader import DownloaderPool
from nzbfs.files import (get_nzbfs_filepath, load_nzbfs_file, RegularFile,
                         RegularFileHandle, NZBFS_FILENAME_RE)

log = logging.getLogger(__name__)


class NzbFs(fuse.Operations, fuse.LoggingMixIn):
    def __init__(self, config_file, *args, **kwargs):
        super(NzbFs, self).__init__(*args, **kwargs)

        self.config_file = config_file
        config = ConfigParser.SafeConfigParser({
            'process_nzb_script': 'nzbfs-process-nzb',
            'threads': '4',
            'port':  '119',
            'ssl': 'false'
        })
        config.read(config_file)

        self.db_root = config.get('nzbfs', 'db_root')
        self.process_nzb_script = config.get('nzbfs', 'process_nzb_script')
        self.post_process_script = config.get('nzbfs', 'post_process_script')

        self._downloaders = {}
        for section in config.sections():
            if section.startswith('/'):
                num_threads = int(config.get(section, 'threads'))
                server = config.get(section, 'server')
                port = config.getint(section, 'port')
                ssl = config.getboolean(section, 'ssl')
                username = config.get(section, 'username')
                password = config.get(section, 'password')
                self._downloaders[section] = DownloaderPool(
                    num_threads, server, port, ssl, username, password)

        self._attr_lock = threading.Lock()
        if self.total_files is None:
            self.total_files = 0
        if self.total_size is None:
            self.total_size = 0

        self._loaded_files = weakref.WeakValueDictionary()
        self._loaded_files_lock = threading.Lock()
        self._open_handles = {}
        self._open_handles_lock = threading.Lock()
        self._last_fh = 0

    def init(self, path=''):
        for downloader in self._downloaders.itervalues():
            downloader.start()

    def _get_downloader(self, path):
        longest_prefix_len = 0
        longest_prefix_downloader = None
        for prefix, downloader in self._downloaders.iteritems():
            if path.startswith(prefix) and len(prefix) > longest_prefix_len:
                longest_prefix_len = len(prefix)
                longest_prefix_downloader = downloader
        if longest_prefix_len == 0:
            log.error('No downloader!')
        return longest_prefix_downloader

    # TODO: Replace with a file?
    def _db_root_property(attr):
        def _get_attr(self):
            try:
                ret = xattr.getxattr(self.db_root, 'user.nzbfs.' + attr)
                return int(ret)
            except IOError:
                return None

        def _set_attr(self, value):
            return xattr.setxattr(
                self.db_root, 'user.nzbfs.' + attr, str(value))

        return property(_get_attr, _set_attr)

    total_files = _db_root_property('total_files')
    total_size = _db_root_property('total_size')

    def load_file(self, path):
        nzbfs_filepath, file_size = get_nzbfs_filepath(self.db_root + path)
        if file_size and not path.endswith('.nzbfs'):
            with self._loaded_files_lock:
                if path in self._loaded_files:
                    return self._loaded_files[path]
                else:
                    nzbfs_file = load_nzbfs_file(nzbfs_filepath)
                    nzbfs_file.orig_nzbfs_filepath = nzbfs_filepath
                    self._loaded_files[path] = nzbfs_file
                    return nzbfs_file
        else:
            return RegularFile(self.db_root + path)

    def access(self, path, mode):
        nzbfs_filepath, file_size = get_nzbfs_filepath(self.db_root + path)
        if not os.access(nzbfs_filepath, mode):
            raise fuse.FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        nzbfs_filepath, file_size = get_nzbfs_filepath(self.db_root + path)
        return os.chmod(nzbfs_filepath, mode)

    def chown(self, path, uid, gid):
        nzbfs_filepath, file_size = get_nzbfs_filepath(self.db_root + path)
        return os.chown(nzbfs_filepath, uid, gid)

    def create(self, path, mode, fi=None):
        nzbfs_filepath, file_size = get_nzbfs_filepath(self.db_root + path)
        fd = os.open(nzbfs_filepath, os.O_WRONLY | os.O_CREAT, mode)
        with self._open_handles_lock:
            self._last_fh += 1
            self._open_handles[self._last_fh] = RegularFileHandle(fd)
            return self._last_fh

    def flush(self, path, fh):
        return self._open_handles[fh].flush()

    def fsync(self, path, datasync, fh):
        return self._open_handles[fh].fsync()

    def getattr(self, path, fh=None):
        nzbfs_filepath, file_size = get_nzbfs_filepath(self.db_root + path)
        st = os.lstat(nzbfs_filepath)

        d = {
            key: getattr(st, key)
            for key in ('st_atime', 'st_ctime', 'st_gid', 'st_mode',
                        'st_mtime', 'st_nlink', 'st_size', 'st_uid')
        }

        if stat.S_ISREG(st.st_mode):
            if file_size:
                d['st_size'] = file_size
        d['st_blocks'] = d['st_size'] / 512

        return d

    def getxattr(self, path, name, position=0):
        nzbfs_filepath, file_size = get_nzbfs_filepath(self.db_root + path)
        try:
            return xattr.getxattr(nzbfs_filepath, name)
        except IOError:
            return ''

    def link(self, target, source):
        nzbfs_filepath, file_size = get_nzbfs_filepath(self.db_root + source)
        return os.link(nzbfs_filepath, self.db_root + target)

    def listxattr(self, path):
        nzbfs_filepath, file_size = get_nzbfs_filepath(self.db_root + path)
        return xattr.listxattr(nzbfs_filepath)

    def mkdir(self, path, mode):
        nzbfs_filepath, file_size = get_nzbfs_filepath(self.db_root + path)
        return os.mkdir(nzbfs_filepath, mode)

    def mknod(self, path, mode, dev):
        nzbfs_filepath, file_size = get_nzbfs_filepath(self.db_root + path)
        return os.mknod(nzbfs_filepath, mode, dev)

    def open(self, path, flags):
        downloader = self._get_downloader(path)
        handle = self.load_file(path).open(flags, downloader)
        with self._open_handles_lock:
            self._last_fh += 1
            self._open_handles[self._last_fh] = handle
            return self._last_fh

    def read(self, path, size, offset, fh):
        return self._open_handles[fh].read(size, offset)

    def readdir(self, path, fh):
        yield '.'
        yield '..'
        for name in os.listdir(self.db_root + path):
            match = NZBFS_FILENAME_RE.search(name)
            if match:
                yield match.group(1)
            else:
                yield name

    def readlink(self, path):
        return os.readlink(self.db_root + path)

    def release(self, path, fh):
        ret = self._open_handles[fh].release()

        file = self._loaded_files.get(path)
        if file:
            if path.endswith('.tmp-autorename'):
                file.save(self.db_root + file.filename)
                os.unlink(file.orig_nzbfs_filepath)
            else:
                savedpath = file.save(self.db_root + path)
                if savedpath != file.orig_nzbfs_filepath:
                    os.unlink(file.orig_nzbfs_filepath)

        del self._open_handles[fh]

        if path.endswith('.nzb') or path.endswith('.nzb.gz'):
            subprocess.check_call([self.process_nzb_script,
                                   self.db_root + path])
            subprocess.Popen([self.post_process_script,
                              path.replace('.nzb', '')])

        return ret

    def removexattr(self, path, name):
        nzbfs_filepath, file_size = get_nzbfs_filepath(self.db_root + path)
        return xattr.removexattr(nzbfs_filepath, name)

    def rename(self, oldpath, newpath):
        nzbfs_oldpath, file_size = get_nzbfs_filepath(self.db_root + oldpath)
        if file_size:
            os.rename(nzbfs_oldpath, '%s%s-%d.nzbfs' %
                      (self.db_root, newpath, file_size))
        else:
            os.rename(nzbfs_oldpath, self.db_root + newpath)

        if newpath.endswith('.nzb') or newpath.endswith('.nzb.gz'):
            subprocess.check_call([self.process_nzb_script,
                                   self.db_root + newpath])
            subprocess.Popen([self.post_process_script,
                              newpath.replace('.nzb', '')])

    def rmdir(self, path):
        return os.rmdir(self.db_root + path)

    def setxattr(self, path, name, value, options, position=0):
        nzbfs_filepath, file_size = get_nzbfs_filepath(self.db_root + path)
        return xattr.setxattr(nzbfs_filepath, name, value)

    def statfs(self, path):
        return {
            'f_files': self.total_files,
            'f_blocks': self.total_size // 512,
            'f_bsize': 512,
        }

    def symlink(self, target, source):
        os.symlink(source, self.db_root + target)

    def truncate(self, path, length, fh=None):
        nzbfs_filepath, file_size = get_nzbfs_filepath(self.db_root + path)
        with open(nzbfs_filepath, 'r+') as f:
            f.truncate(length)

    def unlink(self, path):
        nzbfs_filepath, file_size = get_nzbfs_filepath(self.db_root + path)
        try:
            if file_size:
                with self._attr_lock:
                    self.total_files -= 1
                    self.total_size -= file_size
        finally:
            return os.unlink(nzbfs_filepath)

    def utimens(self, path, times=None):
        nzbfs_filepath, file_size = get_nzbfs_filepath(self.db_root + path)
        return os.utime(nzbfs_filepath, times)

    def write(self, path, data, offset, fh):
        return self._open_handles[fh].write(data, offset)

    def check(self, path):
        downloader = self._get_downloader(path)
        handle = self.load_file(path).open('r', downloader)
        if handle.check():
            return 0
        else:
            raise fuse.FuseOSError(errno.EIO)
