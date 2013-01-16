import logging
import re

import yenc

# Example: =ybegin part=1 line=128 size=123 name=-=DUMMY=- abc.par
YSPLIT_RE = re.compile(r'([a-zA-Z0-9]+)=')
log = logging.getLogger(__name__)


class YencLineHandler(object):
    STATE_WAITING = 0
    STATE_DECODING = 1

    def __init__(self, file, part):
        self.file = file
        self.part = part
        self._cached_data = None
        self.reset()

    def reset(self):
        self._state = YencLineHandler.STATE_WAITING
        self._decoder = yenc.Decoder()

    def set_data(self, data):
        self._cached_data = data

    def get_data(self):
        if self._cached_data is None:
            self._cached_data = self._decoder.getDecoded()
        return self._cached_data

    def get_size(self):
        return self._decoder.getSize()

    def feed(self, line):
        if line.startswith('=y'):
            log.debug(line)
            self.handle_y(line)
        elif self._state == YencLineHandler.STATE_DECODING and line:
            self._cached_data = None  # Reset data cache
            self._decoder.feed(line)

    def handle_y(self, line):
        fields = ySplit(line)
        with self.file._lock:
            if line.lower().startswith('=ybegin'):
                if 'part' in fields:
                    self.part.number = int(fields['part'])
                else:
                    self.part.number = 1
                self.file.file_size = int(fields['size'])
                self.file.filename = fields['name']
                if not self.file.seen:
                    self.file.seen = True
                    self.file.dirty = True
                self._state = YencLineHandler.STATE_DECODING
            elif line.lower().startswith('=ypart'):
                self.part.begin = int(fields['begin']) - 1
                self.part.bytes = int(fields['end']) - int(fields['begin']) + 1
                if not self.part.seen:
                    self.part.seen = True
                    self.file.dirty = True
                self._state = YencLineHandler.STATE_DECODING
            elif line.lower().startswith('=yend'):
                self._state = YencLineHandler.STATE_WAITING


def ySplit(line, splits=None):
    fields = {}

    if 'name=' in line:
        line, fields['name'] = line.split('name=')

    if splits:
        parts = YSPLIT_RE.split(line, splits)[1:]
    else:
        parts = YSPLIT_RE.split(line)[1:]

    if len(parts) % 2:
        return fields

    for i in range(0, len(parts), 2):
        key, value = parts[i], parts[i + 1]
        fields[key] = value.strip()

    return fields
