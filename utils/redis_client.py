import os
import sys
import redis
from logger import log
import logging

class RedisClient:
    def __init__(self):
        self.redis_client = None

    def setup_redis_client(self):
        """
        Sets up and returns a Redis client instance.
        """
        try:
            self.redis_client = redis.Redis(
                host=os.environ.get('REDIS_HOST', 'localhost'), 
                port=int(os.environ.get('REDIS_PORT', 6379)),   
                db=0,                                           
                decode_responses=True                          
            )
            
            if self.redis_client.ping():
                log("[RedisClient]: Redis Connection established sucessfully.", level=logging.INFO)
            return self.redis_client
        except Exception as e:
            print(f"Error setting up Redis client: {e}")
            sys.exit(1)