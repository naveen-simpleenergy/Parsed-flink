from pyflink.datastream import MapFunction, RuntimeContext
from .data_producer import KafkaDataProducer
from utils import MessagePayload
import json

class KafkaSender(MapFunction):
    def __init__(self, config_dict, topic_path):
        self.config_dict = config_dict
        self.topic_path = topic_path
        self.kafka_producer = None

    def open(self, runtime_context: RuntimeContext):
        self.kafka_producer = KafkaDataProducer(self.config_dict, self.topic_path)

    def map(self, payload : MessagePayload):
        return json.dumps(self.kafka_producer.send_data(payload))
