from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration, Configuration
from pathlib import Path
DBC_FILE_PATH = './dbc_files/SimpleOneGen1_V2_2.dbc'

KAFKA_BROKER = '35.200.225.51:9094,35.200.177.250:9094'  
KAFKA_TOPIC = 'flink_parser'
KAFKA_GROUP_ID = 'flink_consumer_group'
KAFKA_USERNAME='user1'
KAFKA_PASSWORD='XGFcl6zFlw'
OUTPUT_TOPIC='flink_parser'
CONSUMER_GROUP_ID='test-group'
SASL_MECHANISMS='SCRAM-SHA-256'
SECURITY_PROTOCOL='SASL_PLAINTEXT'

def print_with_type(x):
    print(f"Type: {type(x).__name__}, Value: {x}")
    return x

def main():
    # Setup Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    kafka_jar_path = str(Path(__file__).resolve().parent / "jars/flink-sql-connector-kafka-3.4.0-1.20.jar")

    kafka_clients_jar_path = str(Path(__file__).resolve().parent / "jars/kafka-clients-3.4.0.jar")

    config = Configuration()
    config.set_string("pipeline.classpaths", f"file://{kafka_jar_path};file://{kafka_clients_jar_path}")
    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)

    env.add_jars(f"file://{kafka_jar_path}")



    # Define Kafka source
    kafka_source = KafkaSource.builder()\
        .set_bootstrap_servers(KAFKA_BROKER) \
            .set_topics(OUTPUT_TOPIC) \
            .set_group_id(CONSUMER_GROUP_ID) \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_property("security.protocol", SECURITY_PROTOCOL) \
            .set_property("sasl.mechanism", SASL_MECHANISMS) \
            .set_property("enable.auto.commit", "false") \
            .set_property("sasl.jaas.config",
                          f"org.apache.kafka.common.security.scram.ScramLoginModule required "
                          f"username='{KAFKA_USERNAME}' password='{KAFKA_PASSWORD}';") \
            .build()
    
    print("Kafka source setup complete")

    # Create data stream
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(5000))
    data_stream = env.from_source(source=kafka_source, watermark_strategy=watermark_strategy, source_name="Kafka Source")

    # Process and print the stream
    data_stream.map(print_with_type, output_type=Types.STRING()).print()

    # Execute the job
    env.execute("CAN Decoding Job")

if __name__ == "__main__":
    main()