from producer.data_producer import KafkaDataProducer
from producer.producer_stage import ProducerStage
from signal_decoding_stage import CANDecoder
from kafka_config import KafkaConfig
from flink_setup import setup_flink_environment
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy,DeliveryGuaranteer 
from stages.filtering_stage import FaultFilter
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
    kafka_source = KafkaConfig.create_kafka_source()
    print("Kafka source setup complete")

    # Setup CAN decoder
    can_decoder = CANDecoder(DBC_FILE_PATH)
    signal_filter = FaultFilter()
    data_producer = ProducerStage(KafkaDataProducer())

    # Create data stream
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(5000))
    data_stream = env.from_source(source=kafka_source, watermark_strategy=watermark_strategy, source_name="Kafka Source")

    # Process and print the stream
    # data_stream.map(lambda x: print_with_type(can_decoder.decode_can_message(x)), output_type=Types.STRING()).print()

    processed_stream = (
    data_stream
    .map(lambda x: can_decoder.decode_can_message(x), output_type=Types.STRING().set_parallelism(20))  # Preserve dict
    .map(lambda x: signal_filter().filter_faults(x), output_type=Types.STRING().set_parallelism(5))  # Use correct class
    .map(lambda x: data_producer.prepare_output(x), output_type=Types.STRING().set_parallelism(10))  # Use correct class
)

    # Setup Kafka sink
    kafka_sink = KafkaConfig.create_kafka_sink().set_parallelism(10)
    print("Kafka sink setup complete")
    data_stream.sink_to(kafka_sink)


    # Execute the job
    env.execute("CAN Decoding Job")

if __name__ == "__main__":
    main()
