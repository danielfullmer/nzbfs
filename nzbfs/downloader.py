import logging
import nntplib
from Queue import Queue
import socket
import ssl
import threading
import weakref

from nzbfs.utils import MemCache

log = logging.getLogger(__name__)


class DownloaderPool(threading.Thread):
    def __init__(self, num_threads, server, port, ssl, username, password):
        super(DownloaderPool, self).__init__()

        self._num_threads = num_threads
        self._server = server
        self._port = port
        self._ssl = ssl
        self._username = username
        self._password = password

        # A queue of available tasks to process
        self._task_queue = Queue()
        # message_id -> BodyTask
        self._body_tasks = weakref.WeakValueDictionary()

        self._segment_cache = MemCache(20 * 1024 * 1024)
        self._lock = threading.Lock()

        self._threads = []

    def run(self):
        for i in range(self._num_threads):
            thread = DownloaderThread(
                self._task_queue, i, self, self._server, self._port, self._ssl,
                self._username, self._password)
            thread.start()
            self._threads.append(thread)

    def queue(self, part, line_handler_factory):
        """Queue a task to fetch a particular part.

        Returns a queue where updates about ecoded data will be fed. Each tuple
        returned from the queue is of the form: (finished, size, error)

        finished: True/False, if the part is finished downloading/decoding.
        data: The currently decoded data.
        error: Optional exception.
        """
        with self._lock:
            update_queue = Queue()
            cached_data = self._segment_cache.get(part.message_id)

            if cached_data:
                line_handler = line_handler_factory()
                line_handler.set_data(cached_data)
                update_queue.put((True, len(cached_data), None))
                return update_queue, line_handler

            if part.message_id in self._body_tasks:
                task = self._body_tasks[part.message_id]
                line_handler = task.line_handler
                if task.state is not None:
                    update_queue.put(task.state)
            else:
                line_handler = line_handler_factory()
                task = BodyTask(line_handler)

                def _update_cache_cb():
                    with self._lock:
                        self._segment_cache.set(
                            part.message_id, line_handler.get_data())
                task.callbacks.append(_update_cache_cb)

                self._task_queue.put(task)
                self._body_tasks[part.message_id] = task

            task.update_queues.append(update_queue)
            return update_queue, line_handler

    def status(self):
        s = []
        for thread in self._threads:
            if thread.cur_task is None:
                s.append('None')
            else:
                s.append(thread.status())
        log.info("Currently processing: " + " ".join(s))


class DownloaderThread(threading.Thread):
    def __init__(self, queue, number, pool, server, port, ssl, username,
                 password):
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
        log.debug(line.strip())
        if code.startswith('4'):
            raise nntplib.NNTPTemporaryError(line)
        if code.startswith('5'):
            raise nntplib.NNTPPermanentError(line)
        return code, line

    def send_line(self, line):
        log.debug(line.strip())
        self._sock.sendall(line)

    def send_group(self, groupname):
        if self.cur_group == groupname:
            return
        self.send_line('group %s\r\n' % groupname)
        code, line = self.get_resp()
        self.cur_group = groupname
        if code != '211':
            log.info('Unexpected response to "group": %s' % line)

    def send_body(self, message_id):
        self.send_line('body <%s>\r\n' % message_id)
        code, line = self.get_resp()
        if code != '222':
            log.info('Unexpected response to "body": %s' % line)

    def send_stat(self, message_id):
        self.send_line('stat <%s>\r\n' % message_id)
        code, line = self.get_resp()
        if code != '223':
            log.info('Unexpected response to "stat": %s' % line)

    def run(self):
        self.connect()
        while True:
            if self.cur_task is None:
                log.info("Thread: %d waiting for a new task", self._number)
                cur_task = self._task_queue.get()
                with self._lock:
                    self.cur_task = cur_task
                log.info("Thread: %d got %s", self._number, self.cur_task)

            self._pool.status()

            try:
                self.cur_task.execute(self)
            except (socket.error, ssl.SSLError), e:
                log.error(e)
                log.info("Reconnecting")
                self.connect()
            except nntplib.NNTPTemporaryError, e:
                log.error(e)
                if e.message.startswith('400'):
                    # RFC3977 says we SHOULD reconnect with an exponentially
                    # increasing delay
                    log.info("Reconnecting")
                    self.connect()
            except Exception, e:
                log.error(e)
            else:
                with self._lock:
                    self.cur_task = None

    def status(self):
        with self._lock:
            if self.cur_task is not None:
                return self.cur_task.line_handler.part.message_id
            else:
                return "None"


class BodyTask(object):
    def __init__(self, line_handler):
        self.callbacks = []
        self.line_handler = line_handler
        self.update_queues = []
        self.state = None

    def reset(self):
        self.line_handler.reset()

    def notify(self, finished, error):
        size = self.line_handler.get_size()
        self.state = (finished, size, error)
        for queue in self.update_queues:
            queue.put(self.state)

    def execute(self, thread):
        thread.send_group(self.line_handler.file.groups[0])
        self.line_handler.reset()
        try:
            thread.send_body(self.line_handler.part.message_id)
        except nntplib.NNTPTemporaryError, e:
            # Don't catch 400 errors here, let it fall through to
            # DownloaderThread.run()
            if e.message.startswith('400'):
                raise

            # Otherwise, notify any listening threads that there is a problem
            # with this article.
            self.notify(True, e.message)
            return
        while True:
            line = thread.get_line()
            if line == '.':
                self.finished()
                return
            if line[:2] == '..':
                line = line[1:]
            self.line_handler.feed(line)

            # Notify on packet end
            if len(thread.recv_lines) == 0:
                self.notify(False, None)

    def finished(self):
        for callback in self.callbacks:
            callback()
        self.notify(True, None)
