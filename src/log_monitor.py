#!/usr/bin/env python3
"""
Simple log monitoring utility to check log file status and sizes
"""

import os
import glob
from datetime import datetime
from humanize import naturalsize

def monitor_logs():
    """Monitor log files and show their status"""
    log_dir = "logs"
    
    if not os.path.exists(log_dir):
        print("No logs directory found.")
        return
    
    print(f"📊 Log File Status - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Get all log files
    log_files = glob.glob(os.path.join(log_dir, "*"))
    
    if not log_files:
        print("No log files found.")
        return
    
    total_size = 0
    for log_file in sorted(log_files):
        if os.path.isfile(log_file):
            size = os.path.getsize(log_file)
            total_size += size
            
            # Get file modification time
            mod_time = datetime.fromtimestamp(os.path.getmtime(log_file))
            
            # File type
            file_type = "JSON" if log_file.endswith('.jsonl') else "TEXT"
            if "error" in os.path.basename(log_file):
                file_type += " (ERROR)"
            
            print(f"📄 {os.path.basename(log_file):<20} | {naturalsize(size):<10} | {file_type:<15} | {mod_time.strftime('%Y-%m-%d %H:%M')}")
    
    print("=" * 60)
    print(f"📊 Total log size: {naturalsize(total_size)}")
    
    # Show recent log entries
    print("\n📋 Recent log entries:")
    print("-" * 40)
    
    app_log = os.path.join(log_dir, "app.log")
    if os.path.exists(app_log):
        try:
            with open(app_log, 'r') as f:
                lines = f.readlines()
                # Show last 5 lines
                for line in lines[-5:]:
                    print(f"  {line.strip()}")
        except Exception as e:
            print(f"  Error reading app.log: {e}")
    else:
        print("  No app.log file found")

def cleanup_old_logs():
    """Clean up old log files (beyond retention period)"""
    log_dir = "logs"
    
    if not os.path.exists(log_dir):
        return
    
    # This is handled automatically by loguru's retention setting
    # but you can add custom cleanup logic here if needed
    print("🧹 Log cleanup is handled automatically by loguru retention settings")
    print("   - Regular logs: 10 days retention")
    print("   - Error logs: 30 days retention") 
    print("   - JSON logs: 7-21 days retention")

if __name__ == "__main__":
    try:
        import humanize
    except ImportError:
        print("Installing humanize for better output...")
        os.system("pip install humanize")
        import humanize
    
    monitor_logs()
    print()
    cleanup_old_logs() 