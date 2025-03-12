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
import cantools
from stages.decoding_stage import CANMessageDecoder
from utils.message_payload import MessagePayload
from utils.config import KafkaConfig
from utils.flink_setup import setup_flink_environment
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from stages.filtering_stage import FaultFilter
from pyflink.common import Duration
import json
import cantools
import sys

### import dbc
DBC_FILE_PATH = './dbc_files/SimpleOneGen1_V2_2.dbc'
dbc_load = cantools.database.load_file(DBC_FILE_PATH) 

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

class ProcessMap(object):

    def __init__(self, dbc_load):
        self.dbc_load = dbc_load

    def map_ghoda(self, value):
        data = json.loads(value)
        
        hex_can_id = data['raw_can_id']
        int_can_id = int(str(hex_can_id), 16) & 0x1FFFFFFF

        decoded_can_ID = dbc_load.get_message_by_frame_id(int_can_id)

        byte_array = bytearray([
            data.pop('byte1'),
            data.pop('byte2'),
            data.pop('byte3'),
            data.pop('byte4'),
            data.pop('byte5'),
            data.pop('byte6'),
            data.pop('byte7'),
            data.pop('byte8')
        ])
        
        decoded_signals =  decoded_can_ID.decode(byte_array,decode_choices=False)
        
        return json.dumps(decoded_signals)


watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(5000))

data_stream = env.from_source(source=kafka_source, watermark_strategy = watermark_strategy, source_name="Kafka Source")

can_decoder = CANMessageDecoder(DBC_FILE_PATH)
fault_filter = FaultFilter(json_file="signalTopic.json")


processed_stream = (
        data_stream
        .map(lambda x: MessagePayload(x), output_type=Types.PICKLED_BYTE_ARRAY())  
        .map(lambda x: can_decoder.execute(x).signal_value_pair, output_type=Types.STRING())   
    )

processed_stream.print() 


env.execute("Kafka Consumer Job - Data Processing")