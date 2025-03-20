from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration
from pathlib import Path
from .config import KafkaConfig

def setup_flink_environment():
    parallelism = KafkaConfig.get_kafka_partition_count()

    kafka_clients_jar_path = str(Path(__file__).resolve().parent / "../jars/kafka-clients-3.4.0.jar")

    config = Configuration()
    config.set_string("pipeline.classpaths", f"file://{kafka_clients_jar_path}")

    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
    env.add_jars(f"file://{kafka_clients_jar_path}")
    env.set_parallelism(parallelism)

    print("Flink environment setup complete")
    return env

