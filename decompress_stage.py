import logging
import zlib
import base64
from interface.stage import Stage
from utils.message_payload import MessagePayload

class DecompresStage(Stage):

    def execute(self,payload:MessagePayload):
        payload.decompressed = self.decompress_base64(payload.compressed)


    def decompress_base64(self,compressed_data_base64:str)->bytes:
        compressed_bytes = base64.b64decode(compressed_data_base64)
        decompressed_data = zlib.decompress(compressed_bytes)
        return decompressed_data