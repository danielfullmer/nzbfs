from collections import deque
import logging
import threading
import os
import re


class MemCache(object):
    def __init__(self, cache_limit):
        self.cache_limit = cache_limit
        self.cache_size = 0
        self._key_deque = deque()
        self._data_dict = dict()
        self._lock = threading.RLock()

    def set(self, key, data):
        with self._lock:
            if key in self._data_dict:
                self.remove(key)

            # Make room
            data_size = len(data)
            while self.cache_size > (self.cache_limit - data_size):
                old_key = self._key_deque.popleft()
                old_data = self._data_dict.pop(old_key)
                self.cache_size -= len(old_data)

            # Add the data
            self._key_deque.append(key)
            self._data_dict[key] = data
            self.cache_size += data_size

    def remove(self, key):
        with self._lock:
            self._key_deque.remove(key)
            data = self._data_dict.pop(key)
            self.cache_size -= len(data)

    def get(self, key):
        with self._lock:
            if key in self._data_dict:
                # Refresh data
                self._key_deque.remove(key)
                self._key_deque.append(key)
                return self._data_dict[key]
            else:
                return None

    def clear(self):
        with self._lock:
            self.cache_size = 0
            self._key_deque = deque()
            self._data_dict = dict()


class DiskCache(object):
    def __init__(self, root):
        self._root = root

    def set(self, key, data):
        f = open(os.path.join(self._root, key), 'w')
        f.write(data)

    def remove(self, key):
        os.unlink(os.path.join(self._root, key))

    def get(self, key):
        try:
            f = open(os.path.join(self._root, key), 'r')
            logging.info("Using a cache file!")
            return f.read()
        except IOError:
            return None


class CacheChain(object):
    def __init__(self, caches):
        self._caches = caches

    def set(self, key, data):
        for cache in self._caches:
            cache.set(key, data)

    def remove(self, key):
        for cache in self._caches:
            cache.remove(key)

    def get(self, key):
        for i, cache in enumerate(self._caches):
            data = cache.get(key)
            if data is not None:
                for cache2 in self._caches[:i]:
                    cache2.set(key, data)
                return data
        return None


class LearningStringMatcher(object):
    def __init__(self, patterns=None):
        self.patterns = patterns or []

    def match(self, input):
        for pattern in self.patterns:
            m = pattern.match(input)
            if m:
                return m.group(1)
        return None

    def should_match(self, input, expected_output):
        pattern = re.escape(input).replace(re.escape(expected_output), '(.*?)')
        pattern = re.sub(r'\d+\\/\d+', r'\d+/\d+', pattern)
        pattern = re.sub(r'\d+\s*of\s*\d+', r'\d+\s*of\s*\d+', pattern)
        self.patterns.append(re.compile(pattern))
