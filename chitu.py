# --*-- coding:utf-8 --*--

import os
import time

from logbook import Logger

from ziyan.utils.util import get_conf
from ziyan.utils.logbook_wrapper import setup_logger
from lib.Send import Send


log = Logger('start')

def start():
    conf = get_conf('conf/conf.toml')
    setup_logger(conf['log_configuration'])


if __name__ == '__main__':
    start()
    while True:
        time.sleep(5)