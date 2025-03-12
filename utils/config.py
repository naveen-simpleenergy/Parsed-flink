import os
from dotenv import load_dotenv
from pathlib import Path
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaOffsetsInitializer, DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema

load_dotenv()

class KafkaConfig:
    KAFKA_BROKER = os.getenv("PROD_KAFKA_BROKER")
    KAFKA_USERNAME = os.getenv("PROD_KAFKA_USERNAME")
    KAFKA_PASSWORD = os.getenv("PROD_KAFKA_PASSWORD")
    INPUT_TOPIC = os.getenv("INPUT_TOPIC")
    CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID")
    SASL_MECHANISMS = os.getenv("SASL_MECHANISMS", "SCRAM-SHA-256")
    SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL", "SASL_PLAINTEXT")

    @staticmethod
    def create_kafka_source():
        return KafkaSource.builder() \
            .set_bootstrap_servers(KafkaConfig.KAFKA_BROKER) \
            .set_topics(KafkaConfig.INPUT_TOPIC) \
            .set_group_id(KafkaConfig.CONSUMER_GROUP_ID) \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_property("security.protocol", KafkaConfig.SECURITY_PROTOCOL) \
            .set_property("sasl.mechanism", KafkaConfig.SASL_MECHANISMS) \
            .set_property("sasl.jaas.config",
                          f"org.apache.kafka.common.security.scram.ScramLoginModule required "
                          f"username='{KafkaConfig.KAFKA_USERNAME}' password='{KafkaConfig.KAFKA_PASSWORD}';") \
            .set_property("enable.auto.commit", "false")\
            .build()
    
    @staticmethod
    def create_kafka_sink():
        return KafkaSink.builder() \
            .set_bootstrap_servers(KafkaConfig.KAFKA_BROKER) \
            .set_record_serializer(SimpleStringSchema()) \
            .set_value_only_serializer(SimpleStringSchema()) \
            .set_property("security.protocol", KafkaConfig.SECURITY_PROTOCOL) \
            .set_property("sasl.mechanism", KafkaConfig.SASL_MECHANISMS) \
            .set_property("sasl.jaas.config",
                          f"org.apache.kafka.common.security.scram.ScramLoginModule required "
                          f"username='{KafkaConfig.KAFKA_USERNAME}' password='{KafkaConfig.KAFKA_PASSWORD}';") \
            .set_topic(KafkaConfig.OUTPUT_TOPIC) \
            .build()
    
    
