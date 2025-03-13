from producer import KafkaDataProducer, KafkaProducer
from stages import CANMessageDecoder, FaultFilter
from utils import KafkaConfig, MessagePayload, setup_flink_environment
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration
from dotenv import load_dotenv
load_dotenv()

import json
import os
import cantools
import sys
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DBC_FILE_PATH = './dbc_files/SimpleOneGen1_V2_2.dbc'

def process_and_log(payload, kafka_producer):
    result = kafka_producer.send_data(payload)
    logging.info(f"Processed data for VIN {payload.vin}:")
    for topic, data in result.items():
        logging.info(f"  Topic: {topic}")
        logging.info(f"  Data: {json.dumps(data, indent=2)}")
    return result

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

    kafka_producer = KafkaDataProducer(kafka_output_config,"signalTopic.json")

    can_decoder = CANMessageDecoder(DBC_FILE_PATH)
    fault_filter = FaultFilter(json_file="signalTopic.json")

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(5000))
    data_stream = env.from_source(source=kafka_source, watermark_strategy=watermark_strategy, source_name="Kafka Source")

    processed_stream = (data_stream
                        .map(lambda x: MessagePayload(x), output_type=Types.PICKLED_BYTE_ARRAY())  
                        .map(lambda x: can_decoder.execute(x), output_type=Types.PICKLED_BYTE_ARRAY())  
                        .map(lambda x: fault_filter.execute(x), output_type=Types.PICKLED_BYTE_ARRAY())  
                        .map(lambda x: json.dumps(x.filtered_signal_value_pair),output_type=Types.STRING()))
                        

    processed_stream.print() 

    env.execute("Flink parser")

if __name__ == "__main__":
    main()