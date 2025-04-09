import json
from logger import log
from cantools.database.namedsignalvalue import NamedSignalValue
from .base_producer import CustomKafkaProducer
from utils import MessagePayload
import concurrent.futures
import logging

class KafkaDataProducer(CustomKafkaProducer):
    """
    Kafka producer for standard data messages, handling serialization and topic-specific production.
    """
    def __init__(self, config, topic_path):
        super().__init__(config)
        
        with open(topic_path, 'r') as file:
            self.topics = json.load(file)

    def send_data(self, payload: MessagePayload):
        """
        Process and send data to specific Kafka topics based on the data content.
        """
        if payload.error_flag:
            return self.error_data_producer(payload)
 
        vin = payload.vin
        event_time = payload.event_time
        topics_data = self.prepare_topics_data(payload.signal_value_pair, vin, event_time)
                    
        for topic, data in topics_data.items():
            try:
                super().send_data(
                    key=self.create_key(vin, event_time),
                    data=data, 
                    topic=topic
                )
                payload.success_counts += 1
            except Exception as e:
                log(f"[Data Producer]: Failed to send the complete processed data to Kafka of {payload.vin} at {payload.event_time}.", level=logging.ERROR)
                payload.error_flag = True
                return self.error_data_producer(payload)

        super().flush()
        log(f"[Data Producer]: Message batch for VIN {payload.vin} is processed.", level=logging.INFO)
        return topics_data
 
    def prepare_topics_data(self, signal_value_map : dict, vin : str, event_time : int) -> dict:
        """
        Organize data by topics for sending to Kafka.
        """
        topics_data = {}
        for key, value in signal_value_map.items():
            topic = self.topics.get(key, 'default-topic')
            
            if topic == 'default-topic':
                log(f"[Data Producer]: No Topic Found for {key} and value {value}", level=logging.CRITICAL)
            
            if topic not in topics_data:
                topics_data[topic] = {"vin": vin, "event_time": event_time}
            
            topics_data[topic][key] = value
            
        return topics_data
    
    
    def create_key(self, vin, event_time):
        """
        Create a key by combining VIN and event time.
        """
        return f"{vin}_{event_time}"
    
    def error_data_producer(self, payload: MessagePayload):
        """
        Handle error data by sending it to the DLQ topic.
        """  
        super().send_data(data=payload.message_json, topic=payload.dlq_topic)
        log(f"[Data Producer]: {payload.vin} data of event time {payload.event_time} is sent to {payload.dlq_topic} topic", level=logging.INFO)
        
        return {"vin": payload.vin, "event_time": payload.event_time , "error": str(payload.error_tag)}