<<<<<<< HEAD
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

    def execute(self, payload: MessagePayload) -> None:
        """
        Decode a CAN message from the payload.

        Args:
            payload (MessagePayload): The message payload to decode.
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
=======
import json
import cantools
from pyflink.datastream.functions import MapFunction
from interface.stage import Stage  
from utils.message_payload import MessagePayload

class CANMessageDecoder(Stage, MapFunction):  
    def __init__(self, dbc_file_path: str):
        self.dbc = cantools.database.load_file(dbc_file_path)

    def execute(self, message_json: str) -> str:  
        """
        Decode a CAN message from JSON input.

        Args:
            message_json (str): The message payload as JSON string.

        Returns:
            str: Decoded JSON string.
        """
        try:
            message = json.loads(message_json)
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
            message["decoded_signals"] = decoded_signals

            return json.dumps(message)  

        except Exception as e:
            print(f"Error decoding CAN message: {e}")
            return json.dumps({"error": "Decoding failed"})

    def map(self, message_json: str) -> str:  
        return self.execute(message_json)  
>>>>>>> some changes
