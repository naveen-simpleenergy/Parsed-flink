import os
from dotenv import load_dotenv
from pathlib import Path
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSink, KafkaSource
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema

load_dotenv()

class KafkaSourceConfig:  
    KAFKA_BROKER = os.getenv("PROD_KAFKA_BROKER")  
    KAFKA_USERNAME = os.getenv("PROD_KAFKA_USERNAME", "")
    KAFKA_PASSWORD = os.getenv("PROD_KAFKA_PASSWORD", "")
    INPUT_TOPIC = os.getenv("INPUT_TOPIC")
    CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID")
    SASL_MECHANISMS = os.getenv("SASL_MECHANISMS", "SCRAM-SHA-256")
    SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL", "SASL_PLAINTEXT")

    @staticmethod
    def create_kafka_source():
        return KafkaSource.builder() \
            .set_bootstrap_servers(KafkaSourceConfig.KAFKA_BROKER) \
            .set_topics(KafkaSourceConfig.INPUT_TOPIC) \
            .set_group_id(KafkaSourceConfig.CONSUMER_GROUP_ID) \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_property("security.protocol", KafkaSourceConfig.SECURITY_PROTOCOL) \
            .set_property("sasl.mechanism", KafkaSourceConfig.SASL_MECHANISMS) \
            .set_property("enable.auto.commit", "false") \
            .set_property("sasl.jaas.config",
                          f"org.apache.kafka.common.security.scram.ScramLoginModule required "
                          f"username='{KafkaSourceConfig.KAFKA_USERNAME}' password='{KafkaSourceConfig.KAFKA_PASSWORD}';") \
            .build()


class KafkaSinkConfig: 
    KAFKA_BROKER = os.getenv("STAGE_KAFKA_BROKER") 
    KAFKA_USERNAME = os.getenv("STAGE_KAFKA_USERNAME", "")
    KAFKA_PASSWORD = os.getenv("STAGE_KAFKA_PASSWORD", "")
    OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC")
    SASL_MECHANISMS = os.getenv("SASL_MECHANISMS", "SCRAM-SHA-256")
    SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL", "SASL_PLAINTEXT")

    @staticmethod
    def create_kafka_sink():
        return KafkaSink.builder() \
            .set_bootstrap_servers(KafkaSinkConfig.KAFKA_BROKER) \
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(KafkaSinkConfig.OUTPUT_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            ) \
            .set_property("security.protocol", KafkaSinkConfig.SECURITY_PROTOCOL) \
            .set_property("sasl.mechanism", KafkaSinkConfig.SASL_MECHANISMS) \
            .set_property("sasl.jaas.config",
                          f"org.apache.kafka.common.security.scram.ScramLoginModule required "
                          f"username='{KafkaSinkConfig.KAFKA_USERNAME}' "
                          f"password='{KafkaSinkConfig.KAFKA_PASSWORD}';") \
            .build()


class DBCConfig:
    DBC_FILE_PATH = './dbc_files/SimpleOneGen1_V2_2.dbc'

class JarConfig:
    KAFKA_JAR_PATH = str(Path(__file__).resolve().parent / "jars/flink-sql-connector-kafka-3.4.0-1.20.jar")
    KAFKA_CLIENTS_JAR_PATH = str(Path(__file__).resolve().parent / "jars/kafka-clients-3.4.0.jar")
