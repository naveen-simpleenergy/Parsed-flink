import os
from dotenv import load_dotenv
from pathlib import Path
from kafka.admin import KafkaAdminClient
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
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
    def get_kafka_partition_count():
        """Fetch the number of partitions for the Kafka topic with SASL authentication."""
        admin_client = KafkaAdminClient(
            bootstrap_servers=KafkaConfig.KAFKA_BROKER,
            security_protocol=KafkaConfig.SECURITY_PROTOCOL,
            sasl_mechanism=KafkaConfig.SASL_MECHANISMS,
            sasl_plain_username=KafkaConfig.KAFKA_USERNAME,
            sasl_plain_password=KafkaConfig.KAFKA_PASSWORD
        )

        try:
            topic_metadata = admin_client.describe_topics([KafkaConfig.INPUT_TOPIC])

            topic_info = next((topic for topic in topic_metadata if topic['topic'] == KafkaConfig.INPUT_TOPIC), None)

            if topic_info is None:
                raise ValueError(f"Topic {KafkaConfig.INPUT_TOPIC} not found")

            if topic_info['error_code'] != 0:
                raise ValueError(f"Error fetching topic metadata: {topic_info['error_code']}")

            num_partitions = len(topic_info['partitions'])

            return num_partitions
        except Exception as e:
            print(f"Error getting partition count: {str(e)}")
            return None
        finally:
            admin_client.close()

    @staticmethod
    def create_kafka_source():
        print("Kafka source setup complete")
        return KafkaSource.builder() \
            .set_bootstrap_servers(KafkaConfig.KAFKA_BROKER) \
            .set_topics(KafkaConfig.INPUT_TOPIC) \
            .set_group_id(KafkaConfig.CONSUMER_GROUP_ID) \
            .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_property("security.protocol", KafkaConfig.SECURITY_PROTOCOL) \
            .set_property("sasl.mechanism", KafkaConfig.SASL_MECHANISMS) \
            .set_property("sasl.jaas.config",
                          f"org.apache.kafka.common.security.scram.ScramLoginModule required "
                          f"username='{KafkaConfig.KAFKA_USERNAME}' password='{KafkaConfig.KAFKA_PASSWORD}';") \
            .set_property("enable.auto.commit", "true") \
            .build()