from interface.stage import Stage
from signal_decoding_stage import CANDecoder
import json
import cantools
from typing import Dict,str
from utils.message_payload import MessagePayload


# class FilteringStage(Stage):
#     def __init__(self):
#         self.file = 'signalTopic.json'

#     # def execute(self, payload:MessagePayload)->Dict:
#     #     payload.filtered_signals_value_pair = self.filter_signals(payload)
        

#     # def filter_signals(self, payload: Dict[str, float]) -> Dict[str, float]:

#     #     signals_map={}
#     #     for signal_name,signal_value in payload.signal_value_pair.items():
#     #         if "fault" in signal_name.lower() and signal_value != 1: continue
#     #         if "alert" in signal_name.lower() and signal_value != 1: continue
#     #         if "fail" in signal_name.lower() and signal_value != 1: continue
#     #         #if "warning" in signal_name.lower() and signal_value != 1: continue
#     #         if signal_name in self.Fault_Signals_Without_Fault_Keyword and signal_value != 1: continue
                
#     #         signals_map[signal_name] = signal_value
#     #     return signals_map
    

#     def filter_func(input_dict, signal_set):
#     fault_signals = list(set(input_dict.keys()).intersection(signal_set))
#     for fault_signal in fault_signals:
#         if input_dict[fault_signal] == 0:
#             input_dict.pop(fault_signal)
#     return input_dict
                

class FaultFilter:
    def __init__(self, json_file='signalTopic.json'):
        self.fault_signals = self._load_fault_signals(json_file)
        
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




