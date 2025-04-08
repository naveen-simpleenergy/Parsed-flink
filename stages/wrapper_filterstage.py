from pyflink.datastream import MapFunction
from stages.filtering_stage import FaultFilter
from utils import MessagePayload

class FilterWrapper(MapFunction): 
    def __init__(self, json_file: str):
        self.json_file = json_file

    def open(self, runtime_context):
        self.fault_filter = FaultFilter(json_file=self.json_file)

    def map(self, payload : MessagePayload):
        return self.fault_filter.execute(payload=payload)
