import json
from typing import Dict
from utils.message_payload import MessagePayload       
from interface import Stage
from pathlib import Path

class FaultFilter(Stage):
    def __init__(self, json_file):   
        
        self.fault_signals = self._load_fault_signals(json_file)
    
    def execute(self, payload: MessagePayload) -> None:
        """
        Filter out fault signals from the payload data.
        
        Args:
            payload (MessagePayload): The message payload to filter.
        """
        if payload.error_flag:
            return payload

        try:
            payload.filtered_signal_value_pair = self.filter_faults(payload.signal_value_pair)

        except Exception as e:
            print(f'[FaultFilter]: error {e} occurred while filtering {payload.vin} at {payload.event_time}')
            payload.error_tag = e
            payload.error_flag = True

        finally:
            return payload
        
    def _load_fault_signals(self, filepath: str) -> set:
        try:
            with open(filepath, 'r') as f:
                signal_data = json.load(f)
                return {signal for signal, category in signal_data.items() 
                       if category == "Faults"}
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading fault signals: {str(e)}")
            return set()

    def filter_faults(self, input_data: Dict[str, float]) -> Dict[str, float]:
        relevant_faults = self.fault_signals & input_data.keys()
        for fault_signal in relevant_faults:
            if input_data[fault_signal] == 0:
                del input_data[fault_signal]
        
        return input_data




