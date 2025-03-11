from signal_decoding_stage import CANDecoder
from kafka_config import KafkaConfig
from flink_setup import setup_flink_environment
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration

DBC_FILE_PATH = './dbc_files/SimpleOneGen1_V2_2.dbc'

def print_with_type(x):
    print(f"Type: {type(x).__name__}, Value: {x}")
    return x

def main():
    # Setup Flink environment using the abstracted function
    env = setup_flink_environment()

    # Create Kafka source
    kafka_source = KafkaConfig.create_kafka_source()
    print("Kafka source setup complete")

    

    # Create data stream
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(5000))
    data_stream = env.from_source(source=kafka_source, watermark_strategy=watermark_strategy, source_name="Kafka Source")

    # Process and print the stream
    data_stream.map(print_with_type,output_type=Types.STRING()).print()

    # Execute the job
    env.execute("CAN Decoding Job")

if __name__ == "__main__":
    main()
