# from producer.data_producer import KafkaDataProducer
# from producer.producer_stage import ProducerStage
from stages import CANMessageDecoder,FaultFilter
from config import KafkaSourceConfig, KafkaSinkConfig
from flink_setup import setup_flink_environment
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy 
from pyflink.common import Duration

DBC_FILE_PATH = './dbc_files/SimpleOneGen1_V2_2.dbc'


def print_with_type(x):
    type_name = type(x).__name__
    print(f"Message Type: {type_name}")
    print(f"Message Content: {x}")
    return f"Type: {type_name}, Content: {x}"

def main():
    env = setup_flink_environment()
    env.set_parallelism(10)
    kafka_source = KafkaSourceConfig.create_kafka_source()

    print("Kafka source setup complete")
    kafka_config = KafkaSourceConfig()
    print("Kafka config setup complete",kafka_config)

    # Setup CAN decoder
    can_decoder = CANMessageDecoder(DBC_FILE_PATH)
    signal_filter = FaultFilter()
    # data_producer = ProducerStage(KafkaDataProducer(kafka_config, './signalTopic.json'))

    # Create data stream
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(5000))
    data_stream = env.from_source(source=kafka_source, watermark_strategy=watermark_strategy, source_name="Kafka Source")

    # Process and print the stream
    # data_stream.map(lambda x: print_with_type(can_decoder.decode_can_message(x)), output_type=Types.STRING()).print()

    processed_stream = (
    data_stream
    .map(CANMessageDecoder(DBC_FILE_PATH), output_type=Types.STRING()).set_parallelism(20)  
    .map(FaultFilter(), output_type=Types.STRING()).set_parallelism(5)  
)

    # Setup Kafka sink
    kafka_sink = KafkaSinkConfig.create_kafka_sink()
    print("Kafka sink setup complete")
    processed_stream.sink_to(kafka_sink)


    # Execute the job
    env.execute("flink job")

if __name__ == "__main__":
    main()
