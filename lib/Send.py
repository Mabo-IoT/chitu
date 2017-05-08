# --*-- coding:utf-8 --*--


import msgpack
import time
from logbook import Logger
from ziyan.utils.database_wrapper import RedisWrapper, InfluxdbWrapper


log = Logger('Send')

class Send:
    def __init__(self, conf, redis_address):
        self.redis = RedisWrapper(redis_address)
        self.influxdb = InfluxdbWrapper(conf['influxdb'])
        self.data_original = None

    def __unpack(self):
        data_len = self.redis.get_len('data_queue')
        if data_len > 0:
            self.data_original = self.redis.dequeue('data_queue')
            data = msgpack.unpackb(self.data_original)
            measurement = msgpack.unpackb(data['measurement'])
            tags = msgpack.unpackb(data['tag'])
            fields = msgpack.unpackb(data['fields'])
            timestamp = data['time']
            unit = msgpack.unpackb(data['unit'])
            josn_data = [
                {
                    'measurement': measurement,
                    'tags': tags,
                    'time': timestamp,
                    'fields': fields
                 }
            ]
            return josn_data, unit
        else:
            log.info('redis have no data')
            return None, None


    def send(self):
        """
        1.unpack redis data
        2.send data to influxdb
        :return: None
        """
        data_handle, time_precision  = self.__unpack()
        if data_handle:
            self.influxdb.send(data_handle, time_precision)

    def reque_data(self):
        """
        return  data to redis
        :return: 
        """
        self.redis.queue_back('data_queue', self.data_original)


    def run(self):
        """
        1.unpack data
        2.send data:
            if send failed, return data to redis
        :return:
        """
        while True:
            try:
                self.send()
            except Exception as e:
                log.error(e)
                self.reque_data()
                time.sleep(3)
