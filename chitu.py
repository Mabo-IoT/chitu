# --*-- coding:utf-8 --*--

import os
import time

from logbook import Logger
import threading

from ziyan.utils.util import get_conf
from ziyan.utils.logbook_wrapper import setup_logger
from lib.Send import Send

log = Logger('start')


def start():
    conf = get_conf('conf/conf.toml')
    setup_logger(conf['log_configuration'])
    workers = []
    thread_set = dict()
    for redis_address in conf['redis']['address']:
        work = Send(conf, redis_address)
        work.name = 'redis_' + str(redis_address['db'])
        thread = threading.Thread(target=work.run, args=(), kwargs={},
                                  name='%s' % work.name)
        thread.setDaemon(True)
        thread.start()
        workers.append(work)
        thread_set[work.name] = thread

    watch = threading.Thread(target=watchdog, name='watchdog', args=(thread_set, workers))


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
                worker = threading.Thread(target=thread.work, args=(),
                                          kwargs={},
                                          name='%s' % thread.name)
                worker.setDaemon(True)
                worker.start()
                threads_set[thread.name] = worker

        time.sleep(10)


if __name__ == '__main__':
    start()
    while True:
        time.sleep(5)
