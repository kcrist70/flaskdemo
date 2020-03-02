import redis
import time

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



