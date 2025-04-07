from pyflink.datastream import MapFunction
from stages.filtering_stage import FaultFilter
from utils import RedisClient, MessagePayload

class FilterWrapper(MapFunction): 
    def __init__(self, json_file: str):
        self.json_file = json_file
        self.redis_client = None

    def open(self, runtime_context):
        self.redis_client = RedisClient().setup_redis_client()  
        self.fault_filter = FaultFilter(json_file=self.json_file,redis_client=self.redis_client)

    def map(self, payload : MessagePayload):
        return self.fault_filter.execute(payload=payload)
