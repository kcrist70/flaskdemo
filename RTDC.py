#!/usr/bin/python3.6
import logging
import time
from multiprocessing import Process

import redis
from elasticsearch import Elasticsearch, RequestsHttpConnection


def logger():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log = logging.getLogger(__name__)
    return log


logger = logger()
SLEEP_TIME = 5


"""
Real time data cleaning scripts:
    get data from elasticsearch
    clean data to pv,uv for day or month  
"""


class Search(object):
    def __init__(self, es_index_name, es_con, redis_con):
        """

        :param es_index_name:  elasticsearch index name, without date
        :param ip:  elasticsearch server IP
        :param port: elasticsearch server port

        """
        self.es = es_con  # elastic search connector class
        self.redisclass = redis_con     # redis custom connector class
        self.redis = self.redisclass.redis_api
        self.es_name = es_index_name
        self.generate_index = GenerateIndex(es_index_name, self.es)  # generate index class
        self.index_name = self.generate_index.determine_index_exist  # index name with datetime

    def run(self, size=1):
        from_ = self.redisclass.get_index(self.es_name)
        while True:
            body = {"search_after": [from_], "sort": [{"time": "asc"}],
                    "query": {"bool": {"must": {"term": {"operation": "request"}}}}}
            page = self.es.search(index=self.index_name, body=body, size=size)
            handle = HandleLog(page, self.es_name, self.redis)
            if handle.data_length > 0:
                from_ = handle.get_search_after_value
                self.redisclass.insert_index(self.es_name, self.index_name.split("_")[-1],
                                             handle.get_search_after_value)
                logger.info("insert {} data, length {} row".format(self.index_name, handle.data_length))
            else:
                time.sleep(SLEEP_TIME)
                logger.debug("no data, sleep time")
                if self.generate_index.determine_index_datetime():
                    self.redisclass.insert_index(self.es_name, self.index_name.split("_")[-1],
                                                 handle.get_search_after_value)
                    self.index_name = self.generate_index.determine_index_exist

                    from_ = 0
                    logger.info("update month date, the index name  now: {}".format(self.index_name))


class HandleLog(object):
    def __init__(self, page, es_name, redis_conn):
        self.data = page
        self.len = len(self.data["hits"]['hits'])
        self.redis = redis_conn
        self.es_name = es_name
        self.handle_log()

    @property
    def data_length(self):
        return self.len

    @property
    def get_search_after_value(self):
        return self.data["hits"]['hits'][-1]['sort'][0]

    def handle_log(self):
        if self.data_length > 0:
            with self.redis.pipeline(transaction=False) as p:
                # count per month pv
                data = dict()
                for hit_1 in self.data["hits"]["hits"]:
                    month = "-".join(hit_1['_source']["time"].split()[0].split('-')[:-1])
                    robot = hit_1['_source']["robot"]
                    if robot + "_" + month not in data.keys():
                        data[robot + "_" + month] = int()
                    data[robot + "_" + month] += 1
                if data:
                    logger.debug("pv per month data: {}".format(data))
                    for mon_pv_name, mon_pv_value in data.items():
                        p.hincrby(mon_pv_name.split("_")[0], mon_pv_name, mon_pv_value)

                # count per day pv
                data = dict()
                for hit_2 in self.data["hits"]["hits"]:
                    day = hit_2['_source']["time"].split()[0]
                    robot = hit_2['_source']["robot"]
                    if robot + "_" + day not in data.keys():
                        data[robot + "_" + day] = int()
                    data[robot + "_" + day] += 1
                if data:
                    logger.debug("pv per day data: {}".format(data))
                    for day_pv_name, day_pv_value in data.items():
                        p.hincrby(day_pv_name.split("_")[0], day_pv_name, day_pv_value)

                # count per month uv
                data = dict()
                for hit_3 in self.data["hits"]["hits"]:
                    month = "-".join(hit_3['_source']["time"].split()[0].split('-')[:-1])
                    robot = hit_3['_source']["robot"]
                    if robot + "_" + month not in data.keys():
                        data[robot + "_" + month] = set()
                    data[robot + "_" + month].add(hit_3['_source']['uid'])
                if data:
                    logger.debug("uv per month data: {}".format(data))
                    for mon_uv_name, mon_uv_value in data.items():
                        p.sadd("set_" + mon_uv_name, *mon_uv_value)
                p.execute()
            logger.debug("handle_log done")


class GenerateIndex(object):
    def __init__(self, es_name, es):
        self.name = es_name    # string
        self.es = es  # class
        self.time = time.strftime("%Y%m", time.localtime())
        self.index_name = es_name + "_" + self.time

    def determine_index_datetime(self):
        if self.time != time.strftime("%Y%m", time.localtime()):
            self.time = time.strftime("%Y%m", time.localtime())
            self.index_name = self.name + "_" + self.time
            return True
        return False

    @property
    def determine_index_exist(self):
        index_name = self.index_name
        while True:
            if self.es.indices.exists(index_name):
                logger.info("the elasticsearch server created index: {}".format(index_name))
                return index_name
            else:
                logger.error("the elasticsearch server does not have this index name: {}".format(index_name))
                time.sleep(SLEEP_TIME)


class RedisConn(object):
    """
    连接redis类
    管理es索引
    """

    def __init__(self, ip='211.137.43.53', port=6389, password="6RPqfW4BWq3PBta9DRXF"):
        self.conn = redis.Redis(host=ip, port=port, password=password, decode_responses=True)

    @property
    def redis_api(self):
        return self.conn

    def get_index(self, name):
        get_time = time.strftime("%Y%m", time.localtime())
        index_ = self.conn.hget(name, name + "_es_index_" + get_time)
        if index_:
            return index_
        return 0

    def insert_index(self, name, time_, value):
        time_ = time_
        self.conn.hset(name, name + "_es_index_" + time_, value)


def main(es_name, size, es_ip, es_port, redis_ip, redis_port, redis_password):
    es_con = Elasticsearch([es_ip], port=es_port, connection_class=RequestsHttpConnection)
    redis_con = RedisConn(ip=redis_ip, port=redis_port, password=redis_password)
    handle = Search(es_name, es_con, redis_con)
    handle.run(size)


if __name__ == '__main__':
    SIZE = 1000

    ES_NAME = ["cc_portal_faq_log", "cc_portal_intention_log"]
    ES_IP = '211.137.43.53'
    ES_PORT = 9250

    REDIS_IP = '211.137.43.53'
    REDIS_PORT = 6389
    REDIS_PASSWORD = "6RPqfW4BWq3PBta9DRXF"

    p_l = []
    for name in ES_NAME:
        p = Process(target=main, args=(name, SIZE, ES_IP, ES_PORT, REDIS_IP, REDIS_PORT, REDIS_PASSWORD))
        p.daemon = True
        p.start()
        p_l.append(p)
    for p in p_l:
        p.join()

# 6RPqfW4BWq3PBta9DRXF
# 6389
