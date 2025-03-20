import psutil
import os
from datetime import datetime

def monitor_resources(interval=5):
    """Continuous monitoring of CPU and memory usage"""
    process = psutil.Process(os.getpid())
    
    while True:

        cpu_percent = psutil.cpu_percent(interval=interval)
        cpu_times = psutil.cpu_times_percent()
        
        mem_info = psutil.virtual_memory()
        process_mem = process.memory_info()
        
        print(f"\n[{datetime.now().isoformat()}] Metrics:")
        print(f"CPU Usage: {cpu_percent}% (User: {cpu_times.user}%, System: {cpu_times.system}%)")
        print(f"System Memory: {mem_info.percent}% used ({mem_info.used//(1024**2)}MB/{mem_info.total//(1024**3)}GB)")
        print(f"Process Memory: {process_mem.rss//(1024**2)}MB RSS, {process_mem.vms//(1024**2)}MB VMS")