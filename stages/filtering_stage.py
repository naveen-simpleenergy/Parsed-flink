from pyflink.datastream.functions import MapFunction
import json
from typing import Dict
from interface.stage import Stage  

class FaultFilter(Stage, MapFunction):  
    def __init__(self, json_file='signalTopic.json'):
        self.fault_signals = self._load_fault_signals(json_file)

    def execute(self, message_json: str) -> str:  
        """
        Filters out fault signals from a JSON input.

        Args:
            message_json (str): The message payload as JSON string.

        Returns:
            str: Filtered JSON string.
        """
        try:
            message = json.loads(message_json)
            signal_values = message.get("decoded_signals", {})

            filtered_signals = self.filter_faults(signal_values)
            message["filtered_signals"] = filtered_signals

            return json.dumps(message)

        except Exception as e:
            print(f"Error filtering faults: {e}")
            return json.dumps({"error": "Filtering failed"})

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

    def map(self, message_json: str) -> str:  
        return self.execute(message_json)  
