# --*-- coding:utf-8 --*--

from ziyan.utils.database_wrapper import RedisWrapper, InfluxdbWrapper


class Send:
    def __init__(self, conf):
        self.influxdb = InfluxdbWrapper(conf['influxdb'])
        self.data_original = None
        pass

    def que_data(self):
        """take data from redis"""
        self.data_original = data_original
         return

    def unpack(self):
        data= self.data_original

        return data, time_unit

    def send(self):
        """
        send data to influxdb
        :return: 
        """
        data_handle = self.unpack()
        self.influxdb.send(data_handle,time_unit)

    def reque_data(self):


    def run(self):
        """
        1.unpack data
        2.send data:
            if send failed, return data to redis
        :return:
        """
        try:
            pass
        except as:


            self.reque_data()