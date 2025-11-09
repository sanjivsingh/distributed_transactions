import redis
from abc import ABC
from redis_setup import constants, config


class RedisDuplicate(ABC):

    def __init__(self, host : str, port : int, set_name : str) -> None:
        super().__init__()
        self.redis = redis.Redis(host=host, port=port, db=0)
        self.set_name = set_name
        from commons import logger
        self.logger = logger.setup_logger(__name__)
        
    def is_present(self, key : str) -> bool:
        sismember =  self.redis.sismember(self.set_name, key)
        if sismember:
            self.logger.info(f" {key} found in {self.set_name}")
        return sismember

    def set_key(self, key : str) -> bool:
        self.redis.setex(self.set_name, config.configurations[constants.DUPLICATE_TTL],key)
        return True 

    def __del__(self):
        if self.redis:
            self.redis.close()