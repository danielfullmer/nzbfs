import calendar
import errno
import gzip
import logging
import os
import re
import stat
import threading
import weakref

import xattr

from nzbfs import fuse
from nzbfs import rarfile
from nzbfs.downloader import DownloaderPool
from nzbfs.files import (get_opener, load_nzf_file, parse_nzb, RarFsFile,
                         rar_sort, RegularFile, RegularFileHandle, YencFsFile)
from nzbfs.utils import (LearningStringMatcher, is_nzf, get_nzf_attr,
                         set_nzf_attr)

SUBJECT_RE = re.compile(r'^.*"(.*?)".*$')
SAMPLE_RE = re.compile(r'((^|[\W_])sample\d*[\W_])|(-s\.)', re.I)
RAR_RE = re.compile(r'\.(?P<ext>part\d*\.rar|rar|s\d\d|r\d\d|\d\d\d)$', re.I)
RAR_RE_V3 = re.compile(r'\.(?P<ext>part\d*)$', re.I)
log = logging.getLogger(__name__)


class NzbFs(fuse.Operations, fuse.LoggingMixIn):
    def __init__(self, config, *args, **kwargs):
        super(NzbFs, self).__init__(*args, **kwargs)

        self.db_root = config.get('nzbfs', 'db_root')
        if config.has_option('nzbfs', 'post_process'):
            self.post_process_script = config.get('nzbfs', 'post_process')
        else:
            self.post_process_script = False

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
            return get_nzf_attr(self.db_root, attr)

        def _set_attr(self, value):
            return set_nzf_attr(self.db_root, attr, value)

        return property(_get_attr, _set_attr)

    total_files = _db_root_property('total_files')
    total_size = _db_root_property('total_size')

    def load_file(self, path):
        if is_nzf(self.db_root + path):
            return self._load_nzf_file(path)
        else:
            return self._load_reg_file(path)

    def _load_nzf_file(self, path):
        with self._loaded_files_lock:
            if path in self._loaded_files:
                return self._loaded_files[path]
            else:
                with open(self.db_root + path, 'r') as fh:
                    nzf_file = load_nzf_file(fh)
                self._loaded_files[path] = nzf_file
                return nzf_file

    def _load_reg_file(self, path):
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
        st = os.lstat(self.db_root + path)

        d = {
            key: getattr(st, key)
            for key in ('st_atime', 'st_ctime', 'st_gid', 'st_mode',
                        'st_mtime', 'st_nlink', 'st_size', 'st_uid')
        }

        if stat.S_ISREG(st.st_mode):
            nzf_size = get_nzf_attr(self.db_root + path, 'size')
            if nzf_size is not None:
                d['st_size'] = nzf_size
            nzf_mtime = get_nzf_attr(self.db_root + path, 'mtime')
            if nzf_mtime is not None:
                d['st_mtime'] = nzf_mtime
        d['st_blocks'] = d['st_size'] / 512

        return d

    def getxattr(self, path, name, position=0):
        try:
            ret = xattr.getxattr(self.db_root + path, name)
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
            file.save(self.db_root + path)

        del self._open_handles[fh]

        if path.endswith('.nzb') or path.endswith('.nzb.gz'):
            self.process_nzb(path)
            os.unlink(self.db_root + path)

        return ret

    def removexattr(self, path, name):
        return xattr.removexattr(self.db_root + path, name)

    def rename(self, oldpath, newpath):
        ret = os.rename(self.db_root + oldpath, self.db_root + newpath)
        if newpath.endswith('.nzb') or newpath.endswith('.nzb.gz'):
            self.process_nzb(newpath)
            os.unlink(self.db_root + newpath)

    def rmdir(self, path):
        return os.rmdir(self.db_root + path)

    def setxattr(self, path, name, value, options, position=0):
        if name == 'user.nzbfs.cmd':
            return self.command(path, value)
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
            if xattr.getxattr(self.db_root + path, 'user.nzbfs.type') == 'nzb':
                with self._attr_lock:
                    file_size = get_nzf_attr(self.db_root + path, 'size')
                    self.total_files -= 1
                    self.total_size -= file_size
        finally:
            return os.unlink(self.db_root + path)

    def utimens(self, path, times=None):
        return os.utime(self.db_root + path, times)

    def write(self, path, data, offset, fh):
        return self._open_handles[fh].write(data, offset)

    def process_nzb(self, path):
        """Add an nzb to the filesystem."""
        downloader = self._get_downloader(path)

        if path.endswith('.gz'):
            fh = gzip.open("%s/%s" % (self.db_root, path))
            basepath = path[:-7]
        else:
            fh = open("%s/%s" % (self.db_root, path))
            basepath = path[:-4]

        sum_files = sum_size = 0
        matcher = LearningStringMatcher([SUBJECT_RE])

        if not os.path.isdir("%s/%s" % (self.db_root, basepath)):
            os.mkdir("%s/%s" % (self.db_root, basepath))

        for file in parse_nzb(fh, downloader):
            handle = file.open('r', downloader)

            filename = matcher.match(file.subject)
            if filename:
                file.filename = filename
            if filename is None:
                try:
                    handle.read(1)
                except Exception, e:
                    log.exception(e)
                matcher.should_match(file.subject, file.filename)
            log.info(filename)

            # TODO: Get the real filesize if it's not a rar, since we try to
            # extract those.
            #if not RAR_RE.search(file.filename):
            #    handle.read(1)

            filename = file.filename.replace('/', '-')
            file.save("%s/%s/%s" % (self.db_root, basepath, filename))
            sum_files += 1
            sum_size += file.file_size

        with self._attr_lock:
            self.total_files += sum_files
            self.total_size += sum_size

        self.post_process(basepath)

    def post_process(self, path):
        if self.post_process_script:
            os.system('"%s" "%s" &' % (self.post_process_script, path))

    _commands = ['extract', 'extract_rars', 'extract_splits', 'check', 'info']

    def command(self, path, value):
        if value in NzbFs._commands:
            return getattr(self, value)(path)

    def extract(self, path):
        try:
            self.extract_rars(path)
        except NotImplementedError:
            raise fuse.FuseOSError(errno.ENOSYS)

    def extract_rars(self, path):
        downloader = self._get_downloader(path)
        max_files = 1

        rars = [filename
                for filename in os.listdir(self.db_root + path)
                if RAR_RE.search(filename)]

        rar_sets = {}
        for rar in rars:
            rar_set = os.path.splitext(os.path.basename(rar))[0]
            if RAR_RE_V3.search(rar_set):
                rar_set = os.path.splitext(rar_set)[0]
            if not rar_set in rar_sets:
                rar_sets[rar_set] = []
            rar_sets[rar_set].append(rar)

        for rar_set in rar_sets:
            rar_sets[rar_set].sort(rar_sort)
            first_rar_filename = rar_sets[rar_set][0]
            files_dict = {
                rar_filename: self.load_file('%s/%s' % (path, rar_filename))
                for rar_filename in rar_sets[rar_set]
            }

            opener = get_opener(files_dict, downloader)

            x = {
                'default_file_offset': 0,
                'largest_add_size': 0,
                'files_seen': 0
            }

            def info_callback(item):
                if item.type == rarfile.RAR_BLOCK_FILE:
                    if item.add_size > x['largest_add_size']:
                        x['largest_add_size'] = item.add_size

                    if (item.flags & rarfile.RAR_FILE_SPLIT_BEFORE) == 0:
                        if x['default_file_offset'] == 0:
                            x['default_file_offset'] = item.file_offset

                        log.info(item.filename)
                        x['files_seen'] += 1
                        if x['files_seen'] >= max_files:
                            log.info("Done parsing")
                            return True  # Stop parsing

            tmp_rf = rarfile.RarFile(first_rar_filename,
                                     info_callback=info_callback,
                                     opener=opener)
            for ri in tmp_rf.infolist():
                date_time = calendar.timegm(ri.date_time)

                if ri.compress_type == 0x30:
                    rf = RarFsFile(ri.filename, ri.file_size, date_time,
                                   ri.file_offset, x['default_file_offset'],
                                   ri.add_size, x['largest_add_size'],
                                   ri.volume, files_dict)
                else:
                    raise Exception('Extract from compressed rar file %s' %
                                    first_rar_filename)
                rf.save('%s/%s/%s' % (self.db_root, path, ri.filename))

            for rar_filename in rar_sets[rar_set]:
                os.unlink('%s/%s/%s' % (self.db_root, path, rar_filename))

    def check(self, path):
        downloader = self._get_downloader(path)
        handle = self.load_file(path).open('r', downloader)
        if handle.check():
            return 0
        else:
            raise fuse.FuseOSError(errno.EIO)
