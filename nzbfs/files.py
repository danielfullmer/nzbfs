import errno
import gzip
import itertools
import logging
import os
import re
import threading

from nzbfs import fuse
from nzbfs import nzbfs_pb2
from nzbfs.linehandlers import YencLineHandler

NZBFS_FILENAME_RE = re.compile(r'(.*)-(\d+)\.nzbfs$')
MAX_READAHEAD = 2 * 1024 * 1024
log = logging.getLogger(__name__)


class Handle(object):
    def __init__(self, file, downloader):
        self._file = file
        self._downloader = downloader
        self._cur = 0

    def read(self, size, offset=None):
        raise NotImplementedError()

    def tell(self):
        return self._cur

    def seek(self, offset, whence=0):
        if whence == 0:
            self._cur = offset
        elif whence == 1:
            self._cur += offset
        elif whence == 2:
            self._cur = self._file.file_size + offset

    def close(self):
        pass

    def flush(self):
        return 0

    def fsync(self):
        return 0

    def release(self):
        return 0


class RegularFile(object):
    @staticmethod
    def _mode2flags(mode):
        #if mode == 'a':
        if mode[0] == 'r':
            return os.O_RDONLY
        elif mode[:2] == 'w+':
            return os.O_WRONLY
        elif mode[0] == 'w':
            return os.O_RDWR

    def __init__(self, path):
        self._path = path

    def open(self, flags, downloader=None):
        if isinstance(flags, str):
            flags = RegularFile._mode2flags(flags)
        fd = os.open(self._path, flags)
        return RegularFileHandle(fd)


class RegularFileHandle(Handle):
    def __init__(self, fd):
        self._fd = fd
        self._rwlock = threading.Lock()

    def flush(self):
        return os.fsync(self._fd)

    def fsync(self):
        return os.fsync(self._fd)

    def seek(self, offset, whence=0):
        os.lseek(self._fd, offset, whence)

    def read(self, size, offset=None):
        with self._rwlock:
            if offset:
                os.lseek(self._fd, offset, 0)
            return os.read(self._fd, size)

    def release(self):
        return os.close(self._fd)

    def write(self, data, offset=None):
        with self._rwlock:
            if offset:
                os.lseek(self._fd, offset, 0)
            return os.write(self._fd, data)

    def check(self):
        return True


class YencFsFile(object):
    def __init__(self, subject=None, poster=None, date_time=0, groups=[],
                 parts=[]):
        self.subject = subject
        self.filename = None
        self.poster = poster
        self.mtime = date_time
        self.groups = groups
        self.parts = parts
        self.file_size = sum(part.bytes for part in parts)
        self.seen = False

        self.dirty = True
        self._lock = threading.RLock()
        self._last_read_offset = 0  # Where the last read left off.
        self._readahead = 1

    def add_part(self, part):
        with self._lock:
            self.parts.append(part)
            self.parts.sort(key=lambda part: part.number)
            self.file_size += part.bytes
            self.dirty = True

    def _guess_part(self, offset):
        assert 0 <= offset

        with self._lock:
            # lower_* <= part/offset <= upper_*
            lower_i = 0
            lower_offset = 0
            upper_i = len(self.parts) - 1
            upper_offset = self.file_size - 1
            for i, part in enumerate(self.parts):
                if part.seen:
                    if part.begin + part.bytes <= offset:
                        lower_i = i + 1
                        lower_offset = part.begin + part.bytes
                    elif part.begin <= offset:
                        lower_i = i
                        lower_offset = part.begin
                    else:
                        break
            for i, part in reverse_enumerate(self.parts):
                if part.seen:
                    if offset < part.begin:
                        upper_i = i - 1
                        upper_offset = part.begin - 1
                    elif offset < part.begin + part.bytes:
                        upper_i = i
                        upper_offset = part.begin + part.bytes - 1
                    else:
                        break

            if lower_offset >= self.file_size:
                return len(self.parts) - 1

            part_size = (upper_offset - lower_offset) / (upper_i - lower_i + 1)
            slice_i = (offset - lower_offset) // part_size
            return lower_i + slice_i

    def load(self, pb):
        self.filename = pb.filename
        self.file_size = pb.file_size
        self.mtime = pb.mtime
        self.seen = pb.seen
        self.subject = pb.subject
        self.poster = pb.poster
        self.groups = pb.groups
        self.parts = pb.parts
        self.dirty = False

    def dump(self):
        pb = nzbfs_pb2.File()
        pb.type = nzbfs_pb2.File.YENC
        pb.filename = self.filename
        pb.file_size = self.file_size
        pb.mtime = self.mtime
        pb.seen = self.seen
        pb.subject = self.subject
        pb.poster = self.poster
        for group in self.groups:
            pb.groups.append(group)
        pb.parts.extend(self.parts)
        return pb

    def save(self, path):
        with self._lock:
            if self.dirty:
                realpath = '%s-%s.nzbfs' % (
                    path, str(self.file_size))
                with gzip.open(realpath, 'w') as fh:
                    fh.write(self.dump().SerializeToString())
                os.utime(realpath, (-1, self.mtime))
                self.dirty = False

                return realpath

    def open(self, mode, downloader):
        return YencFsHandle(self, downloader)


class YencFsHandle(Handle):
    def __init__(self, *args, **kwargs):
        super(YencFsHandle, self).__init__(*args, **kwargs)

    def _fetch(self, part):
        update_queue, line_handler = self._downloader.queue(
            part, lambda: YencLineHandler(self._file, part))
        return update_queue, line_handler

    def prefetch(self, size, offset=None):
        if offset is None:
            offset = self._cur

        with self._file._lock:
            assert self._file._readahead > 0

            if abs(offset - self._file._last_read_offset) < 100 * 1024:
                self._file._last_read_offset = offset + size

                if size * self._file._readahead > MAX_READAHEAD:
                    size = MAX_READAHEAD
                elif size == 128 * 1024:
                    # The current OS-level readahead limit.
                    size *= self._file._readahead
                    log.info("Increasing readahead")
                    self._file._readahead *= 2
            else:
                self._file._last_read_offset = offset + size

                if self._file._readahead > 1:
                    log.info("Reducing readahead")
                    self._file._readahead /= 2

        base_i = self._file._guess_part(offset)
        i = 0
        with self._file._lock:
            while size > 0 and base_i + i < len(self._file.parts):
                self._fetch(self._file.parts[base_i + i])
                size -= self._file.parts[base_i + i].bytes
                i += 1

    def read(self, size, offset=None):
        if offset is None:
            offset = self._cur

        if offset >= self._file.file_size:
            log.error("Reading past file end.")
            return '\0' * size  # TODO: Should I be doing this?

        # Start fetching ones we probably need.
        self.prefetch(size, offset)

        data = ''
        tries = 5
        size_remaining = size
        while size_remaining > 0 and tries > 0:
            if self._file.seen and offset >= self._file.file_size:
                log.error("Reading past file end.")
                break
            part_i = self._file._guess_part(offset)
            if part_i >= len(self._file.parts):
                part_i = len(self._file.parts) - 1
            part = self._file.parts[part_i]
            update_queue, line_handler = self._fetch(part)

            while True:
                part_finished, part_size, part_error = update_queue.get()
                if part_error:
                    log.error(part_error)
                    raise fuse.FuseOSError(errno.EIO)
                elif part_finished:
                    break
                elif (part.begin is not None
                      and part_size >= offset - part.begin + size_remaining):
                    break

            newdata = line_handler.get_data()

            assert part.begin is not None
            if part.begin <= offset < part.begin + part.bytes:
                part_offset = offset - part.begin
                newdata = newdata[part_offset:part_offset + size_remaining]
                offset += len(newdata)
                size_remaining -= len(newdata)
                data += newdata
            else:
                log.info(
                    'Missed part. Offset: %d part: %s' % (offset, part))
                tries -= 1

        self._cur = offset
        return data

    def check(self):
        task = self._downloader.queue_stat(self._file)
        task.wait()
        return task.complete


class RarFsFile(object):
    def __init__(self, filename='', file_size=0, mtime=0, first_file_offset=0,
                 default_file_offset=0, first_add_size=0, default_add_size=0,
                 first_volume_num=0, sub_files=None):
        self.filename = filename
        self.file_size = file_size
        self.mtime = mtime
        self.first_file_offset = first_file_offset
        self.default_file_offset = default_file_offset
        self.first_add_size = first_add_size
        self.default_add_size = default_add_size
        self.first_volume_num = first_volume_num
        if sub_files:
            self.sub_files = sub_files
        else:
            self.sub_files = []

        self.dirty = True
        self._lock = threading.RLock()

    def load(self, pb):
        self.filename = pb.filename
        self.file_size = pb.file_size
        self.mtime = pb.mtime
        self.first_file_offset = pb.first_file_offset
        self.default_file_offset = pb.default_file_offset
        self.first_add_size = pb.first_add_size
        self.default_add_size = pb.default_add_size
        self.dirty = False

        self.sub_files = []
        for f in pb.sub_files:
            yf = YencFsFile()
            yf.load(f)
            self.sub_files.append(yf)

    def dump(self):
        pb = nzbfs_pb2.File()
        pb.type = nzbfs_pb2.File.RAR
        pb.filename = self.filename
        pb.file_size = self.file_size
        pb.mtime = self.mtime
        pb.first_file_offset = self.first_file_offset
        pb.default_file_offset = self.default_file_offset
        pb.first_add_size = self.first_add_size
        pb.default_add_size = self.default_add_size

        for yf in self.sub_files:
            pb.sub_files.extend([yf.dump()])

        return pb

    def save(self, path):
        with self._lock:
            if self.dirty or any(file.dirty for file in self.sub_files):
                realpath = '%s-%s.nzbfs' % (
                    path, str(self.file_size))
                with gzip.open(realpath, 'w') as fh:
                    fh.write(self.dump().SerializeToString())
                os.utime(realpath, (-1, self.mtime))
                self.dirty = False

                return realpath

    def open(self, mode, downloader):
        return RarFsHandle(self, downloader)


class RarFsHandle(Handle):
    def read(self, size, offset=None):
        if offset is None:
            offset = self._cur

        data = ''
        i = self._file.first_volume_num
        if offset >= self._file.first_add_size:
            i += (1 + (offset - self._file.first_add_size) //
                  self._file.default_add_size)

        assert i < len(self._file.sub_files)
        while size > 0 and i < len(self._file.sub_files):
            if offset >= self._file.file_size:
                break

            if offset < self._file.first_add_size:
                file_offset = offset
                file_length = min(self._file.first_add_size - file_offset,
                                  size)
                outer_file_offset = file_offset + self._file.first_file_offset

            if offset >= self._file.first_add_size:
                file_offset = offset - self._file.first_add_size
                file_offset -= ((i - self._file.first_volume_num - 1) *
                                self._file.default_add_size)
                file_length = min(self._file.default_add_size - file_offset,
                                  size)
                outer_file_offset = (file_offset +
                                     self._file.default_file_offset)

            if (outer_file_offset < 0 or
                    outer_file_offset >= self._file.sub_files[i].file_size):

                log.error("offset: %d", offset)
                log.error("file_offset: %d", file_offset)
                log.error("outer_file_offset: %d", outer_file_offset)
                log.error("file_length: %d", file_length)
                log.error("i: %d", i)
                log.error("first_volume_num: %d",
                          self._file.first_volume_num)
                log.error("first_add_size: %d", self._file.first_add_size)
                log.error("default_add_size: %d",
                          self._file.default_add_size)
                log.error("first_file_offset: %d",
                          self._file.first_file_offset)
                log.error("default_file_offset: %d",
                          self._file.default_file_offset)

            handle = self._file.sub_files[i].open('r', self._downloader)
            newdata = handle.read(file_length, outer_file_offset)
            offset += len(newdata)
            size -= len(newdata)
            i += 1
            data += newdata

        self._cur = offset
        return data

    def check(self):
        stat_tasks = [self._downloader.queue_stat(sub_file)
                      for sub_file in self._file.sub_files]
        for task in stat_tasks:
            task.wait()
            if not task.complete:
                return False
        return True


def get_nzbfs_filepath(path):
    dirname = os.path.dirname(path)
    basename = os.path.basename(path)

    # Check to see if this is currently a .nzbfs file
    match = NZBFS_FILENAME_RE.search(path)
    if match:
        return path, 0

    # Otherwise, search for an nzbfs file that matches this path
    for filename in os.listdir(dirname):
        if filename.startswith(basename):
            match = NZBFS_FILENAME_RE.search(filename)
            if match:
                return os.path.join(dirname, filename), int(match.group(2))

    # If not found, just use the original file
    return path, 0


def load_nzbfs_file(path):
    with gzip.open(path, 'r') as fh:
        pb = nzbfs_pb2.File.FromString(fh.read())

    if pb.type == nzbfs_pb2.File.YENC:
        f = YencFsFile()
    elif pb.type == nzbfs_pb2.File.RAR:
        f = RarFsFile()
    else:
        raise Exception('Unknown nzbfs file type.')

    f.load(pb)

    return f


def reverse_enumerate(l):
    return itertools.izip(xrange(len(l) - 1, -1, -1), reversed(l))
