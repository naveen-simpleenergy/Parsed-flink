from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration
from pathlib import Path

def setup_flink_environment(parallelism=1):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)

    kafka_jar_path = str(Path(__file__).resolve().parent / "../jars/flink-sql-connector-kafka-3.4.0-1.20.jar")
    kafka_clients_jar_path = str(Path(__file__).resolve().parent / "../jars/kafka-clients-3.4.0.jar")

    config = Configuration()
    config.set_string("pipeline.classpaths", f"file://{kafka_jar_path};file://{kafka_clients_jar_path}")
    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
    env.add_jars(f"file://{kafka_jar_path}")

    print("Flink environment setup complete")
    return env