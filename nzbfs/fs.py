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
from nzbfs.files import load_nzbfs_file, RegularFile, RegularFileHandle
from nzbfs.utils import is_nzbfs_file, get_nzbfs_attr, set_nzbfs_attr

log = logging.getLogger(__name__)


class NzbFs(fuse.Operations, fuse.LoggingMixIn):
    def __init__(self, config_file, mount_root, *args, **kwargs):
        super(NzbFs, self).__init__(*args, **kwargs)

        self.config_file = config_file
        config = ConfigParser.SafeConfigParser({
            'post_process_script': 'nzbfs-process-nzb',
            'threads': '4',
            'port':  '119',
            'ssl': 'false'
        })
        config.read(config_file)

        self.mount_root = mount_root
        self.db_root = config.get('nzbfs', 'db_root')
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

    def _db_root_property(attr):
        def _get_attr(self):
            return get_nzbfs_attr(self.db_root, attr)

        def _set_attr(self, value):
            return set_nzbfs_attr(self.db_root, attr, value)

        return property(_get_attr, _set_attr)

    total_files = _db_root_property('total_files')
    total_size = _db_root_property('total_size')

    def _raw_path(self, path):
        if path.endswith('-raw'):
            return path[:-4], True
        else:
            return path, False

    def load_file(self, path):
        path, israw = self._raw_path(path)
        if is_nzbfs_file(self.db_root + path) and not israw:
            with self._loaded_files_lock:
                if path in self._loaded_files:
                    return self._loaded_files[path]
                else:
                    nzbfs_file = load_nzbfs_file(self.db_root + path)
                    self._loaded_files[path] = nzbfs_file
                    return nzbfs_file
        else:
            return RegularFile(self.db_root + path)

    def access(self, path, mode):
        if not os.access(self.db_root + path, mode):
            raise fuse.FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        return os.chmod(self.db_root + path, mode)

    def chown(self, path, uid, gid):
        return os.chown(self.db_root + path, uid, gid)

    def create(self, path, mode, fi=None):
        fd = os.open(self.db_root + path, os.O_WRONLY | os.O_CREAT, mode)
        with self._open_handles_lock:
            self._last_fh += 1
            self._open_handles[self._last_fh] = RegularFileHandle(fd)
            return self._last_fh

    def flush(self, path, fh):
        return self._open_handles[fh].flush()

    def fsync(self, path, datasync, fh):
        return self._open_handles[fh].fsync()

    def getattr(self, path, fh=None):
        path, israw = self._raw_path(path)

        st = os.lstat(self.db_root + path)

        d = {
            key: getattr(st, key)
            for key in ('st_atime', 'st_ctime', 'st_gid', 'st_mode',
                        'st_mtime', 'st_nlink', 'st_size', 'st_uid')
        }

        if stat.S_ISREG(st.st_mode):
            file_size = get_nzbfs_attr(self.db_root + path, 'size')
            if file_size is not None:
                d['st_size'] = file_size
            file_mtime = get_nzbfs_attr(self.db_root + path, 'mtime')
            if file_mtime is not None:
                d['st_mtime'] = file_mtime
        d['st_blocks'] = d['st_size'] / 512

        return d

    def getxattr(self, path, name, position=0):
        try:
            return xattr.getxattr(self.db_root + path, name)
        except IOError:
            return ''

    def link(self, target, source):
        return os.link(self.db_root + source, self.db_root + target)

    def listxattr(self, path):
        return xattr.listxattr(self.db_root + path)

    def mkdir(self, path, mode):
        return os.mkdir(self.db_root + path, mode)

    def mknod(self, path, mode, dev):
        return os.mknod(self.db_root + path, mode, dev)

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
        for name in os.listdir('%s/%s' % (self.db_root, path)):
            yield name

    def readlink(self, path):
        return os.readlink('%s/%s' % (self.db_root, path))

    def release(self, path, fh):
        ret = self._open_handles[fh].release()

        file = self._loaded_files.get(path)
        if file:
            if path.endswith('.tmp-autorename'):
                # TODO check this logic
                desired_filepath = self.db_root + file.filename
            else:
                desired_filepath = self.db_root + path

            if file.broken:
                desired_filepath += '-broken'

            file.save(desired_filepath)

        del self._open_handles[fh]

        if path.endswith('.nzb') or path.endswith('.nzb.gz'):
            subprocess.check_call([self.process_nzb_script,
                                   self.db_root + path])
            subprocess.Popen([self.post_process_script,
                              path.replace('.nzb', '')])

        return ret

    def removexattr(self, path, name):
        return xattr.removexattr(self.db_root + path, name)

    def rename(self, oldpath, newpath):
        os.rename(self.db_root + oldpath, self.db_root + newpath)
        if newpath.endswith('.nzb') or newpath.endswith('.nzb.gz'):
            subprocess.check_call([self.process_nzb_script,
                                   self.db_root + newpath])
            subprocess.Popen([self.post_process_script,
                              newpath.replace('.nzb', '')])

    def rmdir(self, path):
        return os.rmdir(self.db_root + path)

    def setxattr(self, path, name, value, options, position=0):
        return xattr.setxattr(self.db_root + path, name, value)

    def statfs(self, path):
        return {
            'f_files': self.total_files,
            'f_blocks': self.total_size // 512,
            'f_bsize': 512,
        }

    def symlink(self, target, source):
        return os.symlink(self.db_root + source, self.db_root + target)

    def truncate(self, path, length, fh=None):
        with open(self.db_root + path, 'r+') as f:
            f.truncate(length)

    def unlink(self, path):
        try:
            if is_nzbfs_file(self.db_root + path, 'user.nzbfs.type'):
                with self._attr_lock:
                    file_size = get_nzbfs_attr(self.db_root + path, 'size')
                    self.total_files -= 1
                    self.total_size -= file_size
        finally:
            return os.unlink(self.db_root + path)

    def utimens(self, path, times=None):
        return os.utime(self.db_root + path, times)

    def write(self, path, data, offset, fh):
        return self._open_handles[fh].write(data, offset)

    def check(self, path):
        downloader = self._get_downloader(path)
        handle = self.load_file(path).open('r', downloader)
        if handle.check():
            return 0
        else:
            raise fuse.FuseOSError(errno.EIO)
