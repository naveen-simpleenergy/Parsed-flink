import json
import cantools
from interface import Stage
from utils.message_payload import MessagePayload

class CANMessageDecoder(Stage):
    def __init__(self, dbc_file_path: str):
        """
        Initialize the CANMessageDecoder with a DBC file.

        Args:
            dbc_file_path (str): Path to the DBC file.
        """
        self.dbc = cantools.database.load_file(dbc_file_path)

    def decode(self, payload: MessagePayload) -> None:
        """
        Decode a CAN message from the payload.

        Args:
            payload (MessagePayload): The message payload to decode.
        """
        message = payload.message_json
        can_id_hex = message.get("raw_can_id")
        
        # Check if CAN ID is known
        can_id_29bit = int(str(can_id_hex), 16) & 0x1FFFFFFF
        decoded_message = self.dbc.get_message_by_frame_id(can_id_29bit)
        
        # Convert raw data to bytearray
        data_bytes = bytearray([
            message.get(f"byte{i+1}", 0) for i in range(8)
        ])
        
        # Decode the CAN message
        decoded_signals = decoded_message.decode(data_bytes, decode_choices=False)
        
        # Update the payload with decoded signals
        payload.signal_value_pair = decoded_signals