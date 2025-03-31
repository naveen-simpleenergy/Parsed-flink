from pyflink.datastream import MapFunction, RuntimeContext
from .data_producer import KafkaDataProducer
import json
from pathlib import Path

class KafkaSender(MapFunction):
    def __init__(self, config_dict, topic_path):
        self.config_dict = config_dict
        self.topic_path = topic_path
        self.kafka_producer = None

    def open(self, runtime_context: RuntimeContext):
        self.kafka_producer = KafkaDataProducer(self.config_dict, self.topic_path)

    def map(self, value):
        return json.dumps(self.kafka_producer.send_data(value))
