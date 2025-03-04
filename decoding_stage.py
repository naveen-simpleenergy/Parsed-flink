import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pathlib import Path
from pyflink.common.watermark_strategy import WatermarkStrategy
from datetime import timedelta
from pyflink.common import Duration
from pyflink.common import Configuration
import json
from dotenv import load_dotenv
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
INPUT_TOPIC = os.getenv("INPUT_TOPIC")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID")
SASL_MECHANISMS = os.getenv("SASL_MECHANISMS", "SCRAM-SHA-256")  
SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL", "SASL_PLAINTEXT")

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

kafka_jar_path = str(Path(__file__).resolve().parent / "jars/flink-sql-connector-kafka-3.4.0-1.20.jar")

kafka_clients_jar_path = str(Path(__file__).resolve().parent / "jars/kafka-clients-3.4.0.jar")

config = Configuration()
config.set_string("pipeline.classpaths", f"file://{kafka_jar_path};file://{kafka_clients_jar_path}")
env = StreamExecutionEnvironment.get_execution_environment(configuration=config)

env.add_jars(f"file://{kafka_jar_path}")


print(f"environment setup complete")

# Configure Kafka Source with SASL Authentication
kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers(KAFKA_BROKER) \
    .set_topics(INPUT_TOPIC) \
    .set_group_id(CONSUMER_GROUP_ID) \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .set_property("security.protocol", SECURITY_PROTOCOL) \
    .set_property("sasl.mechanism", SASL_MECHANISMS) \
    .set_property("sasl.jaas.config",
                  f"org.apache.kafka.common.security.scram.ScramLoginModule required "
                  f"username='{KAFKA_USERNAME}' password='{KAFKA_PASSWORD}';") \
    .build()

print(f"Kafka source setup complete")

class LimitedPrintMap(object):
    def __init__(self):
        self.counter = 0

    def map(self, value):
        if self.counter < 1:
            self.counter += 1
            return f"Received: {value}"
        else:
            return None

class HexToIntMap(object):
    def map(self, value):
        data = json.loads(value)
        hex_can_id = data['raw_can_id']
        int_can_id = int(hex_can_id, 16)
        data['raw_can_id'] = int_can_id
        return json.dumps(data)

watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(5000))

data_stream = env.from_source(source=kafka_source, watermark_strategy = watermark_strategy, source_name="Kafka Source")

# data_stream.map(LimitedPrintMap().map, output_type=Types.STRING()) \
#     .filter(lambda x: x is not None) \
#     .print()

data_stream.map(HexToIntMap().map, output_type=Types.STRING()) \
    .print()

env.execute("Kafka Consumer Job - Hex to Int Conversion")
