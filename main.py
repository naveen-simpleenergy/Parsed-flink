from producer import KafkaDataProducer, KafkaSender 
from stages import CANMessageDecoder, FaultFilter
from utils import KafkaConfig, MessagePayload, setup_flink_environment
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration
from dotenv import load_dotenv
load_dotenv()

import os
import sys

DBC_FILE_PATH = './dbc_files/SimpleOneGen1_V2_2.dbc'

def main():
    env = setup_flink_environment(parallelism=1)
    kafka_source = KafkaConfig.create_kafka_source()

    kafka_output_config = {
            'brokers': os.getenv("STAGE_KAFKA_BROKER").split(','),
            'security_protocol': os.getenv("SECURITY_PROTOCOL"),
            'sasl_mechanism': os.getenv("SASL_MECHANISMS"),
            'sasl_username': os.getenv("STAGE_KAFKA_USERNAME"),
            'sasl_password': os.getenv("STAGE_KAFKA_PASSWORD"),
        }
    


    can_decoder = CANMessageDecoder(DBC_FILE_PATH)
    fault_filter = FaultFilter(json_file="signalTopic.json")

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(5000))
    data_stream = env.from_source(source=kafka_source, watermark_strategy=watermark_strategy, source_name="Kafka Source")

    processed_stream = (data_stream
                        .map(lambda x: MessagePayload(x), output_type=Types.PICKLED_BYTE_ARRAY())  
                        .map(lambda x: can_decoder.execute(x), output_type=Types.PICKLED_BYTE_ARRAY())  
                        .map(lambda x: fault_filter.execute(x), output_type=Types.PICKLED_BYTE_ARRAY())  
                        .map(KafkaSender(kafka_output_config, "signalTopic.json"),output_type=Types.STRING()))
                        
    processed_stream.print() 

    env.execute("Flink parser")

if __name__ == "__main__":
    main()