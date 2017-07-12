# --*-- coding:utf-8 --*--


import time
import sys
import msgpack
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
            # get data from redis
            self.data_original = self.redis.dequeue('data_queue')

            # unpack data
            data = msgpack.unpackb(self.data_original)
            data = msgpack.unpackb(self.data_original, encoding='utf-8')

            # get influxdb send data
            measurement = data['measurement']
            tags = data['tags']
            fields = data['fields']
            timestamp = data['time']
            unit = data['unit']

            if data.get('heartbeat'):
                tags['Heartbeat'] = 'yes'

            # influxdb data structure
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
            time.sleep(5)
            return None, None


    def send(self):
        """
        1.unpack redis data
        2.send data to influxdb
        :return: None
        """
        data_handle, time_precision = self.__unpack()
        if data_handle:
            info = self.influxdb.send(data_handle, time_precision)
            if info:
                log.info('send data to inflxudb.{}, {}'.format(data_handle[0]['measurement'], info))
            else:
                raise Exception("\n Can't connect influxdb")

    def reque_data(self):
        """
        return  data to redis
        :return: 
        """
        self.redis.queue_back('data_queue', self.data_original)

    def run(self, **kwargs):
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
                # can't connect to influxdb then repush data to redis
                self.reque_data()
                time.sleep(3)
            kwargs['record'].thread_signal[kwargs['name']] = time.time()
