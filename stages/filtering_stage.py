from pyflink.datastream.functions import MapFunction
import json
from typing import Dict
from interface.stage import Stage  
from utils.message_payload import MessagePayload

class FaultFilter(Stage, MapFunction):  
    def __init__(self, json_file='signalTopic.json'):
        self.fault_signals = self._load_fault_signals(json_file)

    def execute(self, payload: MessagePayload) -> str:  
        """
        Filters out fault signals from a JSON input.

        Args:
            message_json (str): The message payload as JSON string.
        """
        payload.filtered_signal_value_pair = self.filter_faults(payload.signal_value_pair)

    def _load_fault_signals(self, filepath: str) -> set:
        try:
            with open(filepath, 'r') as f:
                signal_data = json.load(f)
                return {signal for signal, category in signal_data.items() if category == "Faults"}
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading fault signals: {str(e)}")
            return set()

    def filter_faults(self, input_data: Dict[str, float]) -> Dict[str, float]:
        relevant_faults = self.fault_signals & input_data.keys()
        for fault_signal in relevant_faults:
            if input_data[fault_signal] == 0:
                del input_data[fault_signal]
        
        return input_data 
