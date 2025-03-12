from abc import ABC, abstractmethod
from typing import Any

class ProducerInterface(ABC):
    """
    Abstract base class representing a producer in a processing pipeline.
    """

    @abstractmethod
    def send_data(self, data: Any):
        """
        Send data to the specified destination.

        Args:
            data (Any): The data to be sent.
        Returns:
            str: Message by the producer
        """
        pass
