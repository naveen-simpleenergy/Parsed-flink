from kafka import KafkaProducer
import kafka
import json
from logger import log  
import time
import psutil
import logging
import sys


class CustomKafkaProducer():
    """
    Base class for Kafka producers, providing common functionalities for producing messages.
    
    Attributes:
        producer (Producer): Confluent Kafka Producer instance.
    """
    def __init__(self, config):
        """
        Initialize the KafkaProducer with the list of broker addresses.

        Args:
            brokers (List[str]): A list of Kafka broker addresses.
        """
        self.producer = KafkaProducer(
            bootstrap_servers=config['brokers'],
            security_protocol=config['security_protocol'],
            sasl_mechanism=config['sasl_mechanism'],
            sasl_plain_username=config['sasl_username'],
            sasl_plain_password=config['sasl_password'],
            linger_ms=10,                  
            batch_size=32768,                      
            max_in_flight_requests_per_connection=5,  
            acks=1,                      
            retries=5,                     
            buffer_memory=67108864
        )

    def delivery_report(self, err, msg): 
        if err is not None:
            if err.code() == kafka.errors.OffsetNotAvailableError:
                log(f"[Producer]: No offset stored for topic {msg.topic()}, partition {msg.partition()}.", level=logging.WARNING)
            else:
                log("[Producer]: Message delivery failed.", level=logging.ERROR, error=err)
        else:
            log(f"[Producer]: Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}", level=logging.INFO)

    def send_data(self, data, topic, key=None):
        try:
            encoded_data = json.dumps(data).encode('utf-8')
            encoded_key = json.dumps(key).encode('utf-8') if key is not None else None
            future = self.producer.send(topic, encoded_data, key=encoded_key)

            future.add_callback(lambda meta: log(
                f"Delivered to {meta.topic}[{meta.partition}] @ {meta.offset}"
            ))
            future.add_errback(lambda exc: log(
                f"Delivery failed: {exc}", level=logging.ERROR
            ))
            
        except Exception as e:
            log("[Producer] Critical Error", level=logging.CRITICAL, exception=e)
            raise

