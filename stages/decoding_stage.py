import json
import cantools
from interface.stage import Stage  
from utils.message_payload import MessagePayload

class CANMessageDecoder(Stage):  
    def __init__(self, dbc_file_path: str):
        self.dbc = cantools.database.load_file(dbc_file_path)

    def execute(self, payload: MessagePayload) -> MessagePayload:  
        """
        Decode a CAN message from JSON input.

        Args:
            message_json (str): The message payload as JSON string.

        """
        message = payload.message_json
        can_id_hex = message.get("raw_can_id")

        can_id_29bit = int(str(can_id_hex), 16) & 0x1FFFFFFF
        decoded_message = self.dbc.get_message_by_frame_id(can_id_29bit)

        data_bytes = bytearray([
            message.get(f"byte{i+1}", 0) for i in range(8)
        ])

        decoded_signals = decoded_message.decode(data_bytes, decode_choices=False)
        payload.signal_value_pair = decoded_signals 
        return payload


