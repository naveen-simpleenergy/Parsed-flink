from abc import ABC, abstractmethod
from typing import Any

class Stage(ABC):
    """
    Abstract base class representing a stage in a processing pipeline.
    """

    @abstractmethod
    def execute(self, message: Any) -> Any:
        """
        Execute the stage with the given message.

        Args:
            message (Any): The message to be processed by the stage.

        Returns:
            Any: The result of processing the message.
        """
        pass
