"""
Windows Task Scheduler Setup for ETL Pipeline

This script creates a Windows Task Scheduler task to run the ETL scheduler automatically.

Usage:
    python setup_windows_scheduler.py
    
This will:
1. Create a task named "ETL Pipeline Scheduler"
2. Run it at system startup
3. Run it daily to ensure it stays running
4. Log all output to logs/scheduler.log
"""

import subprocess
import os
import sys
from pathlib import Path

def setup_task_scheduler():
    """Create Windows Task Scheduler task for ETL pipeline"""
    
    # Get the absolute path to this project
    project_path = os.path.dirname(os.path.abspath(__file__))
    python_exe = sys.executable
    scheduler_script = os.path.join(project_path, "scheduler.py")
    
    # Create the task XML definition
    task_xml = f"""<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Date>2026-01-07T00:00:00</Date>
    <Author>Financial ETL</Author>
    <Description>Automatically runs the financial ETL pipeline on schedule</Description>
    <URI>\\ETL Pipeline Scheduler</URI>
  </RegistrationInfo>
  <Triggers>
    <BootTrigger>
      <Enabled>true</Enabled>
      <Delay>PT5M</Delay>
    </BootTrigger>
    <TimeTrigger>
      <Enabled>true</Enabled>
      <ScheduleByDay>
        <DaysInterval>1</DaysInterval>
        <StartBoundary>2026-01-07T00:00:00</StartBoundary>
      </ScheduleByDay>
    </TimeTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>S-1-5-18</UserId>
      <RunLevel>HighestAvailable</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IfTaskIsRunning</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <Duration>PT10M</Duration>
      <WaitTimeout>PT1H</WaitTimeout>
      <StopOnIdleEnd>true</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <DisallowStartOnRemoteAppSession>false</DisallowStartOnRemoteAppSession>
    <UseUnifiedSchedulingEngine>true</UseUnifiedSchedulingEngine>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <DeleteExpiredTaskAfter>PT0S</DeleteExpiredTaskAfter>
    <RestartCount>3</RestartCount>
    <RestartInterval>PT1M</RestartInterval>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>"{python_exe}"</Command>
      <Arguments>"{scheduler_script}"</Arguments>
      <WorkingDirectory>{project_path}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>"""

    # Save task XML to temp file
    temp_xml = os.path.join(project_path, "etl_task.xml")
    with open(temp_xml, 'w') as f:
        f.write(task_xml)
    
    print("=" * 70)
    print("Setting up Windows Task Scheduler for ETL Pipeline")
    print("=" * 70)
    print(f"\nPython: {python_exe}")
    print(f"Project: {project_path}")
    print(f"Script: {scheduler_script}")
    print(f"\nCreating task from: {temp_xml}")
    
    try:
        # Create the scheduled task using schtasks
        cmd = [
            "schtasks",
            "/create",
            "/tn", "ETL Pipeline Scheduler",
            "/xml", temp_xml,
            "/f"  # Force overwrite if exists
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("\n[SUCCESS] Task created successfully!")
            print("\nTask Details:")
            print("  Name: ETL Pipeline Scheduler")
            print("  Triggers:")
            print("    - At system startup (5 minute delay)")
            print("    - Daily at midnight")
            print("  Run with: Highest privileges")
            print("  Status: Enabled")
            print(f"\nLogs will be saved to: {os.path.join(project_path, 'logs/scheduler.log')}")
            
            # Clean up temp file
            if os.path.exists(temp_xml):
                os.remove(temp_xml)
            
            print("\n" + "=" * 70)
            print("NEXT STEPS:")
            print("=" * 70)
            print("1. Verify the task:")
            print("   schtasks /query /tn 'ETL Pipeline Scheduler' /v")
            print("\n2. View scheduled tasks:")
            print("   Open Task Scheduler (taskmgr) -> Task Scheduler Library")
            print("\n3. Run immediately (for testing):")
            print("   schtasks /run /tn 'ETL Pipeline Scheduler'")
            print("\n4. To remove the task:")
            print("   schtasks /delete /tn 'ETL Pipeline Scheduler' /f")
            print("=" * 70)
            
        else:
            print(f"\n[FAILED] {result.stderr}")
            # Clean up
            if os.path.exists(temp_xml):
                os.remove(temp_xml)
            
    except Exception as e:
        print(f"\n[ERROR] {str(e)}")
        if os.path.exists(temp_xml):
            os.remove(temp_xml)


if __name__ == "__main__":
    # Check if running as administrator
    try:
        import ctypes
        is_admin = ctypes.windll.shell.IsUserAnAdmin()
    except:
        is_admin = False
    
    if not is_admin:
        print("[WARNING] This script should be run as Administrator!")
        print("\nTo run as Administrator:")
        print("1. Open Command Prompt as Administrator")
        print("2. Run: python setup_windows_scheduler.py")
        sys.exit(1)
    
    setup_task_scheduler()
