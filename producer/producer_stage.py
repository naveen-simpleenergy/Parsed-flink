from interface import Stage, ProducerInterface
from typing import Any, Dict

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
