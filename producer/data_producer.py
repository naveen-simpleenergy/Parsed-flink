import json
from logger import log
from cantools.database.namedsignalvalue import NamedSignalValue
from .base_producer import KafkaProducer
from utils.message_payload import MessagePayload
import logging

class KafkaDataProducer(KafkaProducer):
    """
    Kafka producer for standard data messages, handling serialization and topic-specific production.
    """
    def __init__(self, brokers, topic_path):
        super().__init__(brokers)
        
        with open(topic_path, 'r') as file:
            self.topics = json.load(file)

    def send_data(self, payload: MessagePayload):
        """
        Process and send data to specific Kafka topics based on the data content.
        """
        payload.kafka_producer_error = []
        vin = payload.vin
        event_time = payload.event_time
        topics_data = self.prepare_topics_data(payload.filtered_signal_value_pair, vin, event_time)
                    
        for topic, data in topics_data.items():
            try:
                if topic == "Faults":
                    for signal_name, value in data.items():
                        if signal_name in ["vin", "event_time"]: continue
                        
                        if value != 1:
                            log("[Data Producer]: Fault Signal Value is not 1.", level=logging.CRITICAL, payload_data=data)
                        
                        packet = {
                            "vin": vin, 
                            "event_time": event_time,
                            "Signal_Name": signal_name,
                            "Sgnal_Value": value
                        }
                        
                        super().send_data(
                            key=self.create_key(vin, event_time),
                            data=packet, 
                            topic=topic
                        )
                        payload.success_counts += 1
                else:
                    super().send_data(
                        key=self.create_key(vin, event_time),
                        data=data, 
                        topic=topic
                    )
                    payload.success_counts += 1
            except Exception as e:
                log("[Data Producer]: Failed to send the complete processed data to Kafka.", level=logging.ERROR)
                payload.kafka_producer_error.append((data, topic))
        
        super().flush()
        log(f"[Data Producer]: Message batch for VIN {payload.vin} is processed.", level=logging.INFO)
    
    def prepare_topics_data(self, signal_value_map, vin, event_time):
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