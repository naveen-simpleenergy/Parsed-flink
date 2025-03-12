from interface import  Stage, ProducerInterface
from typing import Any, Dict
from pyflink.datastream.functions import RichFlatMapFunction

class FlinkProducerStage(RichFlatMapFunction):
    def __init__(self, config, topic_path):
        self.config = config
        self.topic_path = topic_path
        
    def open(self, runtime_context):
        # Initialize ON WORKER (not during serialization)
        self.producer_stage = ProducerStage(
            KafkaDataProducer(self.config, self.topic_path)
        )
        
    def flat_map(self, value):
        try:
            # Process through your existing pipeline
            self.producer_stage.execute(value)
            yield f"Processed VIN {value.vin}"  # Dummy output to continue stream
        except Exception as e:
            yield f"Error processing {value.vin}: {str(e)}"

class ProducerStage(Stage):
    """
    A pipeline stage that sends data using a Kafka producer, integrating the producer into data processing pipelines.
    """

    def __init__(self, producer: ProducerInterface):
        """
        Initialize ProducerStage with a Kafka producer.

        Args:
            producer (ProducerInterface): A Kafka producer instance.
        """
        self.producer = producer

    def execute(self, payload: Dict) -> Any:
        """
        Execute the stage by sending the provided message using the producer.

        Args:
            message (Any): The message to be sent by the producer.

        Returns:
            Any: Message by the producer.
        """
        self.producer.send_data(payload)
