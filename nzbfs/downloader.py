import logging
import nntplib
from Queue import Queue
import socket
import ssl
import threading
import weakref

from nzbfs.utils import MemCache

class DownloaderPool(threading.Thread):
    def __init__(self, num_threads, server, port, ssl, username, password):
        super(DownloaderPool, self).__init__()

        self._num_threads = num_threads
        self._server = server
        self._port = port
	self._ssl = ssl
        self._username = username
        self._password = password

        self._task_queue = Queue()
        self._body_tasks = weakref.WeakValueDictionary() # message_id -> BodyTask

        # TODO: Make disk cache configurable
        #self._segment_cache = CacheChain((MemCache(5 * 1024 * 1024), DiskCache('/mnt/cache')))
        self._segment_cache = MemCache(20 * 1024 * 1024)
        self._lock = threading.Lock()

	self._threads = []

    def run(self):
        for i in range(self._num_threads):
            thread = DownloaderThread(self._task_queue, i, self, self._server, self._port, self._ssl, self._username, self._password)
	    thread.start()
	    self._threads.append(thread)

    def queue(self, part, line_handler_factory):
        with self._lock:
            cached_result = self._segment_cache.get(part.message_id)
            if (part.begin is None or part.bytes is None) or cached_result is None:
                if part.message_id in self._body_tasks:
                    task = self._body_tasks[part.message_id]
                else:
                    line_handler = line_handler_factory()
                    task = BodyTask(line_handler)
		    def _update_cache_cb():
			with self._lock:
			    self._segment_cache.set(part.message_id, line_handler.get_data())
		    task.callbacks.append(_update_cache_cb)
                    self._task_queue.put(task)
                    self._body_tasks[part.message_id] = task
            else:
                return cached_result

        return task

    def queue_stat(self, file):
        task = StatTask(file)
        self._task_queue.put(task)
        return task

    def status(self):
	s = []
	for thread in self._threads:
	    if thread.cur_task is None:
		s.append('None')
	    else:
		s.append(thread.status())
	logging.info("Currently processing: " + " ".join(s))

class DownloaderThread(threading.Thread):
    def __init__(self, queue, number, pool, server, port, ssl, username, password):
        super(DownloaderThread, self).__init__()
        self._task_queue = queue
	self._number = number
	self._pool = pool
        self._server = server
        self._port = port
	self._ssl = ssl
        self._username = username
        self._password = password

	self.cur_task = None
	self._lock = threading.Lock()

        self.reset()

        self.daemon = True

    def reset(self):
        self._sock = None
        self.cur_group = None
        self.recv_data = ''
        self.recv_lines = []

    def connect(self):
        self.reset()
        self._sock = socket.create_connection((self._server, self._port))
	if self._ssl:
	    self._sock = ssl.wrap_socket(self._sock)

        # Sometimes the NNTP server will hang. We'll just timeout and restart.
        # TODO: Do something smarter here?
	self._sock.settimeout(10.0)

        code, line = self.get_resp()
        if code in ('200', '201'):
            if self._username:
                self.send_line('authinfo user %s\r\n' % self._username)
            else:
                return
        else:
            raise Exception(line)

        code, line = self.get_resp()
        self.send_line('authinfo pass %s\r\n' % self._password)
        code, line = self.get_resp()
        if not code.startswith('2'):
            raise Exception(line)

    def get_line(self):
        while len(self.recv_lines) == 0:
            self.recv_data += self._sock.recv(32768)
            self.recv_lines = self.recv_data.split('\r\n')
            self.recv_data = self.recv_lines.pop()
        return self.recv_lines.pop(0)

    def get_resp(self):
        line = self.get_line()
        code = line.split(None, 1)[0]
        logging.debug(line.strip())
        if code.startswith('4'):
            raise nntplib.NNTPTemporaryError(line)
        if code.startswith('5'):
            raise nntplib.NNTPPermanentError(line)
        return code, line

    def send_line(self, line):
        logging.debug(line.strip())
        self._sock.sendall(line)

    def send_group(self, groupname):
        if self.cur_group == groupname:
            return
        self.send_line('group %s\r\n' % groupname)
        code, line = self.get_resp()
        self.cur_group = groupname
        if code != '211':
            logging.info('Unexpected response to "group": %s' % line)

    def send_body(self, message_id):
        self.send_line('body <%s>\r\n' % message_id)
        code, line = self.get_resp()
        if code != '222':
	    logging.info('Unexpected response to "body": %s' % line)

    def send_stat(self, message_id):
        self.send_line('stat <%s>\r\n' % message_id)
        code, line = self.get_resp()
        if code != '223':
            logging.info('Unexpected response to "stat": %s' % line)

    def run(self):
        self.connect()
        while True:
	    if self.cur_task == None:
		logging.info("Thread: %d waiting for a new task", self._number)
		cur_task = self._task_queue.get()
		with self._lock:
		    self.cur_task = cur_task
		logging.info("Thread: %d got %s", self._number, self.cur_task)

	    self._pool.status()

            try:
                self.cur_task.execute(self)
            except (socket.error, ssl.SSLError, nntplib.NNTPTemporaryError, nntplib.NNTPPermanentError), e:
                logging.error(e)
		logging.info("Reconnecting")
                self.connect()
	    except Exception, e:
		logging.error(e)
	    else:
		with self._lock:
		    self.cur_task = None

    def status(self):
	with self._lock:
	    if self.cur_task is not None:
		return self.cur_task.line_handler.part.message_id
	    else:
		return "None"

class DownloaderTask(object):
    def __init__(self):
        self.cv = threading.Condition()
        self.callbacks = []
        self.done = False
        self.error = None

    def reset(self):
        self.done = False
        self.error = None

    def execute(self, thread):
        raise NotImplementedError()

    def notify(self):
        self.cv.acquire()
        self.cv.notifyAll()
        self.cv.release()

    def wait(self):
        self.cv.acquire()
        self.cv.wait()
        self.cv.release()

    def finished(self):
        self.done = True
        for callback in self.callbacks:
            callback()
        self.notify()

    def send_error(self, error):
        self.error = error
        self.finished()

class StatTask(DownloaderTask):
    def __init__(self, file):
        super(StatTask, self).__init__()
        self._file = file
        self.complete = True

    def reset(self):
        super(StatTask, self).reset()
        self.complete = True

    def execute(self, thread):
        thread.send_group(self._file.groups[0])
        for part in self._file.parts:
            thread.send_line('stat <%s>\r\n' % part.message_id)
        for part in self._file.parts:
            try:
                code, line = thread.get_resp()
                if code != '223':
                    logging.info('Unexpected response to "stat": %s' % line)
            except nntplib.NNTPTemporaryError:
                self.complete = False
                self.finished()
        self.finished()

class BodyTask(DownloaderTask):
    def __init__(self, line_handler):
        super(BodyTask, self).__init__()
        self.line_handler = line_handler

    def reset(self):
        super(BodyTask, self).reset()
        self.line_handler.reset()

    def execute(self, thread):
        thread.send_group(self.line_handler.file.groups[0])
        self.line_handler.reset()
        try:
            thread.send_body(self.line_handler.part.message_id)
        except nntplib.NNTPTemporaryError, e:
            self.send_error(e)
            return
        while True:
            line = thread.get_line()
            if line == '.':
                self.finished()
                return
            if line[:2] == '..':
                line = line[1:]
            self.line_handler.feed(line)
            if len(thread.recv_lines) == 0:
                self.notify()

class MockDownloader(object):
    def __init__(self, article_dir):
        self.article_dir = article_dir

    def queue(self, part, line_handler_factory):
        fh = open('%s/%s' % (self.article_dir, part))
        line_handler = line_handler_factory()
        task = DownloaderTask(line_handler)
        for line in fh:
            line_handler.feed(line)
        task.finished()
