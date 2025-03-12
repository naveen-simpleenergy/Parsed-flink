import os
from logger import log
import logging

class KafkaConfig:
    """
    Configuration class for Kafka-related settings.
    """

    @property
    def KAFKA_TOPIC(self):
        return os.getenv('KAFKA_TOPIC', 'emqx-data')

    @property
    def KAFKA_BROKERS(self):
        return os.getenv('KAFKA_BROKERS', 'localhost:9092').split(',')

    @property
    def KAFKA_GROUP_ID(self):
        return os.getenv('KAFKA_GROUP_ID', 'default_group')

    @property
    def KAFKA_AUTO_OFFSET_RESET(self):
        return os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
    
    @property
    def SECURITY_PROTOCOL(self):
        return os.getenv('SECURITY_PROTOCOL', 'SASL_PLAINTEXT')
    
    @property
    def SASL_MECHANISM(self):
        return os.getenv('SASL_MECHANISM', 'SCRAM-SHA-256')
    
    @property
    def SASL_USERNAME(self):
        return os.getenv('SASL_USERNAME', '')
    
    @property
    def SASL_PASSWORD(self):
        return os.getenv('SASL_PASSWORD', '')
    
    @property
    def config(self):
        def commit_completed(err, partitions):
            if err:
                log(f"Message commit error {str(err)}", level=logging.WARNING, exception=err)
            else:
                for partition in partitions:
                    log(f"Committed partition offsets", level=logging.INFO, Topic=partition.topic, Partition=partition.partition, Offset=partition.offset, Error=partition.error)
        
        return {
            'brokers':self.KAFKA_BROKERS,
            'group_id':self.KAFKA_GROUP_ID,
            'auto_offset_reset':self.KAFKA_AUTO_OFFSET_RESET,
            'security_protocol':self.SECURITY_PROTOCOL,
            'sasl_mechanism':self.SASL_MECHANISM,
            'sasl_username':self.SASL_USERNAME,
            'sasl_password':self.SASL_PASSWORD,
            'on_commit': commit_completed
        }


class PinotConfig:
    """
    Configuration class for Pinot-related settings.
    """

    @property
    def PINOT_CONTROLLER_URL(self):
        return os.getenv('PINOT_CONTROLLER_URL', 'http://localhost:9000')
    
    @property
    def PINOT_TOKEN(self):
        return os.getenv('PINOT_TOKEN', '')
    
    @property
    def REDIS_HOST(self):
        return os.getenv('REDIS_HOST', 'localhost')
    
    @property
    def REDIS_PORT(self):
        return os.getenv('REDIS_PORT', 6379)

