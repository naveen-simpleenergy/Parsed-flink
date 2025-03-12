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

DBC_FILE_PATH = './dbc_files/SimpleOneGen1_V2_2.dbc'

def main():
    env = setup_flink_environment(parallelism=1)
    kafka_source = KafkaConfig.create_kafka_source()
    print("Kafka source setup complete")

    can_decoder = CANMessageDecoder(DBC_FILE_PATH)
    fault_filter = FaultFilter(json_file="signalTopic.json")

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(5000))
    data_stream = env.from_source(source=kafka_source, watermark_strategy=watermark_strategy, source_name="Kafka Source")

    # processed_stream = (
    #     data_stream
    #     .map(lambda x: MessagePayload(x), output_type=Types.PICKLED_BYTE_ARRAY())  
    #     .map(lambda x: can_decoder.execute(x), output_type=Types.PICKLED_BYTE_ARRAY())  
    #     .map(lambda x: fault_filter.execute(x), output_type=Types.PICKLED_BYTE_ARRAY())  
    #     .map(lambda x: x.filtered_signal_value_pair, output_type=Types.STRING()) 
    # )

    # processed_stream.print() 
    
    print("halo")
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

    dbc_load = cantools.database.load_file(DBC_FILE_PATH) 

    data_stream = env.from_source(source=kafka_source, watermark_strategy = watermark_strategy, source_name="Kafka Source")

    process_map = ProcessMap(dbc_load)
    
    data_stream.map(process_map.map_ghoda, output_type=Types.STRING()) \
        .print()

    env.execute("Flink parser")

if __name__ == "__main__":
    main()
