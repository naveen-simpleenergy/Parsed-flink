from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration
from pathlib import Path
from .config import KafkaConfig

def setup_flink_environment():
    parallelism = KafkaConfig.get_kafka_partition_count()
    config = Configuration()
    
    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
    env.set_parallelism(parallelism=2)
    env.enable_checkpointing(30000)
    print("Flink environment setup complete")
    return env

