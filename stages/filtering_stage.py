import json
from typing import Dict
from utils import MessagePayload, RedisClient      
from interface import Stage
from pathlib import Path

class FaultFilter(Stage):
    def __init__(self, json_file : str, redis_client : RedisClient):   
        
        self.fault_signals = self._load_fault_signals(json_file)
        self.redis_client = redis_client
    
    def execute(self, payload: MessagePayload) -> MessagePayload:
        """
        Filter out fault signals from the payload data.
        
        Args:
            payload (MessagePayload): The message payload to filter.
        """
        if payload.error_flag:
            return payload

        try:
            payload.filtered_signal_value_pair = self.filter_faults(payload)

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


    def filter_faults(self, payload: MessagePayload) -> Dict[str, float]:
    
        relevant_faults = self.fault_signals & payload.signal_value_pair.keys()
    
        if not relevant_faults:
            return payload.signal_value_pair

        vin = payload.vin
        stored_faults = self.redis_client.hgetall(f"fstate_{vin}")

        for signal_name in relevant_faults:
            current_value = payload.signal_value_pair[signal_name]
            stored_value = stored_faults.get(signal_name)
            
            if stored_value == current_value:
                del payload.signal_value_pair[signal_name]
            else:
                self.redis_client.hset(f"fstate_{vin}", signal_name, current_value)

        return payload.signal_value_pair



