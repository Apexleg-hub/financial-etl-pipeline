# ETL Pipeline Scheduler Guide

This guide explains how to set up automatic scheduling for your financial ETL pipeline.

## Option 1: APScheduler (Recommended for Development)

APScheduler runs as a Python background process and is the easiest to set up.

### Installation

```bash
pip install APScheduler
```

### Usage

```bash
python scheduler.py
```

The scheduler will start and run in the background. It will:
- Log all activity to `logs/scheduler.log`
- Log to console simultaneously
- Run configured jobs on their schedules
- Keep running until you press Ctrl+C

### Scheduled Jobs

| Job | Schedule | Details |
|-----|----------|---------|
| Stock ETL | Daily 10:00 AM | Alpha Vantage stock data (post-market open) |
| Weather ETL | Every 6 hours | 00:00, 06:00, 12:00, 18:00 UTC |
| Forex ETL | Daily 5:00 PM | Daily forex rates (post-market close) |
| FRED ETL | Weekly Monday 9:00 AM | Weekly economic indicators |
| Finnhub ETL | Daily 11:00 AM | Alternative stock data source |
| Full Sync | Daily 00:00 | All pipelines together (midnight) |

### Monitoring

```bash
# Watch the log in real-time (on Linux/Mac)
tail -f logs/scheduler.log

# On Windows PowerShell
Get-Content logs/scheduler.log -Wait
```

### Stopping the Scheduler

Press `Ctrl+C` in the terminal where the scheduler is running.

---

## Option 2: Windows Task Scheduler (Recommended for Production)

For production deployment on Windows, use Windows Task Scheduler to run the scheduler automatically at system startup.

### Setup

Run this command as Administrator:

```bash
python setup_windows_scheduler.py
```

This will:
1. Create a Windows Task named "ETL Pipeline Scheduler"
2. Configure it to run at system startup (with 5-minute delay)
3. Configure daily triggers to ensure continuous operation
4. Set it to run with highest privileges

### Verification

After setup, verify the task was created:

```bash
# List the task
schtasks /query /tn "ETL Pipeline Scheduler" /v

# Or open Task Scheduler GUI
taskmgr
# Navigate to: Task Scheduler Library → ETL Pipeline Scheduler
```

### Manual Controls

```bash
# Run the task immediately (for testing)
schtasks /run /tn "ETL Pipeline Scheduler"

# Pause the task
schtasks /change /tn "ETL Pipeline Scheduler" /disable

# Resume the task
schtasks /change /tn "ETL Pipeline Scheduler" /enable

# Delete the task
schtasks /delete /tn "ETL Pipeline Scheduler" /f
```

### Logs

- Main scheduler log: `logs/scheduler.log`
- Task output: Available in Windows Task Scheduler → Task History
- ETL pipeline logs: Individual logs in `logs/` directory

---

## Option 3: Docker (For Containerized Deployment)

### Build Image

```bash
docker build -f Dockerfile -t financial-etl-scheduler .
```

### Run Container

```bash
docker run -d \
  --name etl-scheduler \
  -v $(pwd)/logs:/app/logs \
  -e PYTHONUNBUFFERED=1 \
  financial-etl-scheduler \
  python scheduler.py
```

### Monitor Logs

```bash
docker logs -f etl-scheduler
```

---

## Configuration

Edit `scheduler.py` to modify:

1. **Job schedules**: Change `CronTrigger(hour=10, minute=0)` values
2. **Pipeline types**: Modify `args=["stock"]` to run different pipelines
3. **Timeout**: Change `timeout=3600` (seconds) for different max execution time
4. **Grace period**: Adjust `misfire_grace_time=300` for late job tolerance

### Cron Format Examples

```python
# Daily at 10:00 AM
CronTrigger(hour=10, minute=0)

# Every 6 hours at :00 minutes
CronTrigger(hour='0,6,12,18', minute=0)

# Weekly Monday at 9:00 AM
CronTrigger(day_of_week='mon', hour=9, minute=0)

# Every 30 minutes
CronTrigger(minute='*/30')

# Multiple times: 9 AM and 5 PM
CronTrigger(hour='9,17', minute=0)
```

---

## Troubleshooting

### Scheduler not running?

1. Check the log file: `logs/scheduler.log`
2. Verify APScheduler is installed: `pip list | grep APScheduler`
3. Check if Python process is running: `tasklist | findstr python` (Windows)
4. Make sure you're in the project directory

### Jobs not executing?

1. Verify the time in your system matches the schedule
2. Check that `run_etl.py` is executable and in the project root
3. Look for error messages in `logs/scheduler.log`
4. Test manually: `python run_etl.py --pipelines stock`

### Permission errors?

1. For Windows Task Scheduler, run `setup_windows_scheduler.py` as Administrator
2. Ensure the Python executable path is correct
3. Check that the project directory is accessible

### High CPU usage?

- Check if a pipeline is stuck (should timeout after 1 hour)
- Look at `logs/scheduler.log` for stuck jobs
- Increase timeout if pipelines legitimately need more time

---

## Production Recommendations

### For Linux/Unix Servers

Use `systemd` service or `cron`:

```bash
# crontab -e
# Run scheduler at reboot
@reboot cd /path/to/project && python scheduler.py >> logs/scheduler.log 2>&1
```

### For Windows Servers

1. Use `setup_windows_scheduler.py` (recommended)
2. Or use NSSM (Non-Sucking Service Manager) for more control

### For Cloud Deployments

1. Use containerized approach with Docker
2. Deploy to AWS ECS, Azure Container Instances, or Kubernetes
3. Use cloud-native scheduling (AWS EventBridge, Google Cloud Scheduler)

### Monitoring & Alerts

Add to `scheduler.py`:

```python
# Send email on failure
import smtplib
from email.mime.text import MIMEText

def send_alert(subject, message):
    # Your alert logic here
    pass

# Call in error handlers:
# send_alert("ETL Failed", result.stderr)
```

---

## Quick Start

### Development (APScheduler)

```bash
# Install dependencies
pip install APScheduler

# Run scheduler
python scheduler.py

# Monitor logs
tail -f logs/scheduler.log
```

### Production (Windows Task Scheduler)

```bash
# Setup (run as Administrator)
python setup_windows_scheduler.py

# Verify
schtasks /query /tn "ETL Pipeline Scheduler"

# Monitor
# Open Windows Event Viewer or check logs/scheduler.log
```

---

## FAQ

**Q: Will the scheduler survive system restarts?**
A: Yes, with Windows Task Scheduler. With APScheduler, you need to use systemd/cron or run it manually.

**Q: Can I run multiple schedulers?**
A: No, they'll conflict. Use one scheduler per system.

**Q: What if a pipeline is running and the scheduled time arrives?**
A: The job will be skipped. Concurrent runs are prevented.

**Q: Can I manually run a job before its scheduled time?**
A: Yes, use `schtasks /run /tn "ETL Pipeline Scheduler"` or add manual trigger in scheduler.py.

**Q: How do I change the schedule after setup?**
A: Edit `scheduler.py` (CronTrigger values) and restart the scheduler.

---

## Next Steps

1. **Install APScheduler**: `pip install APScheduler`
2. **Test the scheduler**: `python scheduler.py` (Ctrl+C after a few seconds)
3. **Set up production**: Run `python setup_windows_scheduler.py` as Administrator
4. **Verify setup**: Check logs and confirm jobs are running
5. **Configure alerts**: Add email/Slack notifications for failures (optional)

