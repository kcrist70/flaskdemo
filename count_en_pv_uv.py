import logging
import time
import redis
from elasticsearch import Elasticsearch, RequestsHttpConnection


def logger():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log = logging.getLogger(__name__)
    return log

logger = logger()
SLEEP_TIME = 2

class Search:
    def __init__(self, es_cls: Elasticsearch, redis_cls: redis, index_name: list):
        self.es = es_cls
        self.redis = redis_cls
        self.index_name = index_name

    def run(self):
        while True:
            start = time.perf_counter()
            self.aggs()
            finish = time.perf_counter() - start
            logger.info("update finish and sleep {} seconds".format(SLEEP_TIME + finish))
            time.sleep(SLEEP_TIME + finish)

    def aggs(self):
        day_data = self.day_pv
        mon_data = self.mon_pv_and_uv
        self.insert_redis(day_data, mon_data)


    @property
    def day_pv(self):
        body = {
                  "size": 0,
                  "aggs": {
                    "group_by_robot": {
                      "terms": {
                        "field": "robot"
                      }
                    }
                  },
                "query": {
                    "bool": {
                      "filter": {
                        "range": {
                          "time": {
                            "gte": "{} 00:00:00.000".format(time.strftime("%Y-%m-%d",time.localtime())),
                            "lte": "{} 23:59:59.999".format(time.strftime("%Y-%m-%d",time.localtime()))
                          }
                        }
                      }
                    }
                  }
                }
        data = self.es.search(index=self.generate_index, body=body)
        logger.debug("day_pv: {}".format(data))
        return data

    @property
    def mon_pv_and_uv(self):
        body = {
            "size": 0,
            "aggs": {
                "group_by_robot": {
                    "terms": {
                        "field": "robot"
                    },
                    "aggs": {
                        "distinct_by_uid": {
                            "cardinality": {
                                "field": "uid"
                            }
                        }
                    }
                }
            }
        }
        data = self.es.search(index=self.generate_index, body=body)
        logger.debug("mon_pv_and_uv: {}".format(data))
        return data

    def insert_redis(self,day_data,mon_data):
        with self.redis.pipeline() as p:
            for data_dict in day_data['aggregations']['group_by_robot']['buckets']:
                p.hset(data_dict['key'], data_dict['key'] + "_en_pv_" + time.strftime("%Y-%m-%d",time.localtime()), data_dict['doc_count'])
            for data_dict in mon_data['aggregations']['group_by_robot']['buckets']:
                p.hset(data_dict['key'], data_dict['key'] + "_en_pv_" + time.strftime("%Y-%m",time.localtime()), data_dict['doc_count'])
                p.hset(data_dict['key'], data_dict['key'] + "_en_uv_" + time.strftime("%Y-%m",time.localtime()), data_dict['distinct_by_uid']['value'])
            p.execute()

    @property
    def generate_index(self):
        index_name = []
        for name in self.index_name:
            index_name.append(name + "_" +time.strftime("%Y%m", time.localtime()))
        while True:
            if self.es.indices.exists(index_name):
                logger.debug("the elasticsearch server created index: {}".format(index_name))
                return index_name
            logger.error("the elasticsearch server does not have this index name: {}".format(index_name))
            time.sleep(SLEEP_TIME * 10)

class MyES:
    @classmethod
    def connect(cls,es_ip: str or list,es_port):
        if isinstance(es_ip,list):
            es_con = Elasticsearch(hosts=es_ip, port=es_port, connection_class=RequestsHttpConnection)
        else:
            es_con = Elasticsearch(hosts=es_ip.split(","), port=es_port, connection_class=RequestsHttpConnection)
        return es_con



class MyRedis:
    @classmethod
    def connect(cls, ip, port, password):
        conn = redis.Redis(host=ip, port=port, password=password, decode_responses=True)
        return conn


if __name__ == '__main__':
    ES_NAME = ["cc_portal_faq_log", "cc_portal_intention_log"]
    ES_IP = '211.137.43.53'
    ES_PORT = 9250

    REDIS_IP = '211.137.43.53'
    REDIS_PORT = 6389
    REDIS_PASSWORD = "6RPqfW4BWq3PBta9DRXF"

    es_con = MyES.connect(es_ip=ES_IP, es_port=ES_PORT)
    redis_con = MyRedis.connect(ip=REDIS_IP, port=REDIS_PORT, password=REDIS_PASSWORD)
    task = Search(es_cls=es_con, redis_cls=redis_con, index_name=ES_NAME)
    task.run()


"""
{
  "took" : 1,
  "timed_out" : false,
  "_shards" : {
    "total" : 5,
    "successful" : 5,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : {
      "value" : 448,
      "relation" : "eq"
    },
    "max_score" : null,
    "hits" : [ ]
  },
  "aggregations" : {
    "group_by_robot" : {
      "doc_count_error_upper_bound" : 0,
      "sum_other_doc_count" : 0,
      "buckets" : [
        {
          "key" : "robot065",
          "doc_count" : 309,
          "distinct_by_uid" : {
            "value" : 25
          }
        },
        {
          "key" : "robot046",
          "doc_count" : 131,
          "distinct_by_uid" : {
            "value" : 11
          }
        },
        {
          "key" : "robot051",
          "doc_count" : 4,
          "distinct_by_uid" : {
            "value" : 1
          }
        },
        {
          "key" : "robot071",
          "doc_count" : 3,
          "distinct_by_uid" : {
            "value" : 3
          }
        },
        {
          "key" : "robot070",
          "doc_count" : 1,
          "distinct_by_uid" : {
            "value" : 1
          }
        }
      ]
    }
  }
}

"""