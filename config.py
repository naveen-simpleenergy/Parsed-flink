# config.py
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

class KafkaConfig:
    KAFKA_BROKER = os.getenv("KAFKA_BROKER")
    KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
    KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
    INPUT_TOPIC = os.getenv("INPUT_TOPIC")
    CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID")
    SASL_MECHANISMS = os.getenv("SASL_MECHANISMS", "SCRAM-SHA-256")
    SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL", "SASL_PLAINTEXT")

class DBCConfig:
    DBC_FILE_PATH = './dbc_files/SimpleOneGen1_V2_2.dbc'

class JarConfig:
    KAFKA_JAR_PATH = str(Path(__file__).resolve().parent / "jars/flink-sql-connector-kafka-3.4.0-1.20.jar")
    KAFKA_CLIENTS_JAR_PATH = str(Path(__file__).resolve().parent / "jars/kafka-clients-3.4.0.jar")
