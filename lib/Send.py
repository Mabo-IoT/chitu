# --*-- coding:utf-8 --*--

import time
from logbook import Logger

log = Logger('send')

from ziyan.utils.database_wrapper import RedisWrapper, InfluxdbWrapper


class Send:
    def __init__(self, conf, redis_address):
        self.influxdb = InfluxdbWrapper(conf['influxdb'])
        self.redis = RedisWrapper(redis_address)
        self.data_original = None
        pass

    def _unpack(self):
        pass

    def send(self):
        """
        1.unpack redis data
        2.send data to influxdb
        :return: None
        """
        data_handle, time_precision  = self._unpack()
        self.influxdb.send(data_handle, time_precision)
        return None

    def reque_data(self):
        """
        return  data to redis
        :return: 
        """
        self.redis.queue_back('data_queue', self.data_original)
        return None


    def run(self):
        """
        1.unpack data
        2.send data:
            if send failed, return data to redis
        :return:
        """
        try:
            self.send()
        except Exception as e:
            log.error(e)
            self.reque_data()
            time.sleep(3)