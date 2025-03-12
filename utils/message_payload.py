from typing import Dict, Any
import json


class MessagePayload:
    """
    Manages the payload of messages, handling user information, compressed data, and processing errors.

    Attributes:
        username (str): Username associated with the message.
        vin (str): Vehicle Identification Number.
        message_json (Dict[str, Any]): Original message in JSON format.
        compressed (Optional[str]): Base64-encoded compressed data string.
        decompressed (Optional[bytes]): Decompressed data bytes.
        kafka_producer_error (Optional[str]): Error message from Kafka producer, if any.
        parsed (Optional[Any]): Parsed data from the decompressed message.
        can_decoded_data (Optional[Any]): CAN bus decoded data.
        can_decoding_errors (Optional[Dict]): Error message from CAN decoding, if any.
    """

    def __init__(self, binary_message: bytes):
        """
        Constructs a MessagePayload object by decoding a binary JSON message.

        Args:
            binary_message (bytes): The binary message containing a JSON-encoded string.

        Raises:
            json.JSONDecodeError: If the binary message is not properly JSON-encoded.
        """
        json_message = json.loads(binary_message)
        self.message_json = json_message
        self.username = json_message.get('username', json_message.get('vin', None))
        self.vin = json_message.get('username', json_message.get('vin', None))
        
        # Raw Consumer Variables
        self.compressed = json_message.get('payload', None)
        self.decompressed = None
        self.parsed = None
        self.can_decoded_data = None
        self.can_decoding_errors = {
            'dbc': [],
            'others': [],
        }
        
        # Processed Consumer Variables
        self.signal_value_pair = {}
        self.filtered_signal_value_pair = {}
        self.can_id_hex = json_message.get('raw_can_id', None)
        self.event_time = json_message.get('event_time', None)
        
        # Common
        self.success_counts = 0
        self.kafka_producer_error = []

    def set_decompressed_data(self, data: bytes):
        """
        Set the decompressed data.

        Args:
            data (bytes): Decompressed data bytes.
        """
        self.decompressed = data

    def set_parsed_data(self, data: Any):
        """
        Set the parsed data from the decompressed message.

        Args:
            data (Any): Parsed data.
        """
        self.parsed = data

    def set_can_decoded_data(self, data: Any):
        """
        Set the CAN decoded data.

        Args:
            data (Any): CAN decoded data.
        """
        self.can_decoded_data = data

    def set_can_decoding_errors(self, errors: str):
        """
        Set the error message from CAN decoding.

        Args:
            errors (str): CAN decoding error message.
        """
        self.can_decoding_errors = errors

    def set_kafka_producer_error(self, error: str):
        """
        Set the error message from Kafka producer.

        Args:
            error (str): Error message from Kafka producer.
        """
        self.kafka_producer_error = error

    def __str__(self):
        return f"MessagePayload(username={self.username}, vin={self.vin})"