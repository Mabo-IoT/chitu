# --*-- coding:utf-8 --*--

import msgpack
from logbook import Logger
from ziyan.utils.database_wrapper import RedisWrapper, InfluxdbWrapper


log = Logger('Send')

class Send:
    def __init__(self, conf, redis_address):
        self.redis = RedisWrapper(redis_address)

    def unpack(self):
        data_len = self.redis.get_len('data_queue')
        if data_len > 0:
            self.rdata = self.redis.dequeue('data_queue')
            data = msgpack.unpackb(self.rdata)
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
        pass

    def run(self):
        pass
