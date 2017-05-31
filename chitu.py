# --*-- coding:utf-8 --*--

import ctypes
import inspect
import threading
import time

from logbook import Logger
from ziyan.utils.logbook_wrapper import setup_logger
from ziyan.utils.util import get_conf

from lib.Send import Send

log = Logger('start')


def start():
    conf = get_conf('conf/conf.toml')
    setup_logger(conf['log_configuration'])
    workers = []

    thread_set = dict()

    recorder = Maintainer()

    for redis_address in conf['redis']['address']:
        work = Send(conf, redis_address)
        work.name = 'redis_' + str(redis_address['db'])
        thread = threading.Thread(target=work.run, args=(), kwargs={'name': work.name, 'record': recorder},
                                  name='%s' % work.name)
        thread.setDaemon(True)
        thread.start()
        workers.append(work)
        thread_set[work.name] = thread

    recorder.thread_set = thread_set

    watch = threading.Thread(target=watchdog, name='watchdog', args=(thread_set, workers, recorder))
    watch.setDaemon(True)
    watch.start()
    return recorder


def watchdog(*args):
    """
    守护线程
    :param args: 
    :return: None
    """
    threads_name = {thread.name for thread in args[0].values()}
    while True:

        threads = set()

        for item in threading.enumerate():
            threads.add(item.name)

        log.debug('\n' + str(threads) + '\n')

        if threads - {'watchdog', 'MainThread'} != threads_name:
            dead_threads = threads_name - (threads - {'watchdog', 'MainThread'})

            # 获取死去线程的实例集
            dead_threads = [thread for thread in args[1] if thread.name in dead_threads]

            threads_set = dict()

            for thread in dead_threads:
                worker = threading.Thread(target=thread.run, args=(),
                                          kwargs={'name': thread.name, 'record': args[2]},
                                          name='%s' % thread.name)
                worker.setDaemon(True)
                worker.start()
                threads_set[thread.name] = worker

            args[2].thread_set.update(threads_set)

        time.sleep(10)


class Maintainer:
    def __init__(self):
        self.thread_signal = dict()
        self.thread_set = None

    def _async_raise(self, tid, exctype):
        """raises the exception, performs cleanup if needed"""
        tid = ctypes.c_long(tid)
        if not inspect.isclass(exctype):
            exctype = type(exctype)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
        if res == 0:
            log.error("invalid thread id")
        elif res != 1:
            """if it returns a number greater than one, you're in trouble,
            and you should call it again with exc=NULL to revert the effect"""
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            log.error("PyThreadState_SetAsyncExc failed")

    def protect(self):
        for threadname, singal in self.thread_signal.items():
            if time.time() - singal > 1200:
                try:
                    self._async_raise(self.thread_set[threadname].ident, SystemExit)
                    log.warning("\n%s is timeout, kill it" % threadname)
                    self.thread_signal[threadname] = time.time()
                except Exception as e:
                    log.error('\nThere is something wrong')


if __name__ == '__main__':
    record = start()
    while True:
        record.protect()
        time.sleep(5)
