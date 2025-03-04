import json
import cantools

class CANDecoder:
    def __init__(self, dbc_file_path):
        self.dbc = cantools.database.load_file(dbc_file_path)

    def decode_can_message(self, value):
        data = json.loads(value)
        
        hex_can_id = data['raw_can_id']
        int_can_id = int(str(hex_can_id), 16) & 0x1FFFFFFF

        decoded_can_message = self.dbc.get_message_by_frame_id(int_can_id)

        byte_array = bytearray([data.get(f'byte{i}', 0) for i in range(1, 9)])
        
        decoded_signals = decoded_can_message.decode(byte_array, decode_choices=False)
        
        return json.dumps(decoded_signals)
