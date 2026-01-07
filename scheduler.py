"""
ETL Pipeline Scheduler

Automatically runs the financial ETL pipeline on a schedule using APScheduler.

Installation:
    pip install APScheduler

Usage:
    python scheduler.py
"""

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import logging
import subprocess
import sys
import os
from datetime import datetime
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Ensure logs directory exists
Path('logs').mkdir(exist_ok=True)


def run_etl_pipeline(pipeline_type: str = "all"):
    """
    Execute the ETL pipeline
    
    Args:
        pipeline_type: Which pipeline to run (all, stock, weather, forex, fred, finnhub)
    """
    try:
        logger.info(f"Starting ETL pipeline: {pipeline_type}")
        
        # Run the ETL script
        result = subprocess.run(
            [sys.executable, "run_etl.py", "--pipelines", pipeline_type],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        if result.returncode == 0:
            logger.info(f"✓ {pipeline_type.upper()} ETL completed successfully")
            if result.stdout:
                logger.info(f"Output:\n{result.stdout}")
        else:
            logger.error(f"✗ {pipeline_type.upper()} ETL failed")
            logger.error(f"Error output:\n{result.stderr}")
            
    except subprocess.TimeoutExpired:
        logger.error(f"ETL pipeline timeout after 1 hour")
    except Exception as e:
        logger.error(f"Error running ETL pipeline: {str(e)}")


def schedule_pipelines():
    """Configure and start the scheduler"""
    
    scheduler = BackgroundScheduler()
    
    # Schedule daily pipelines
    # Stock ETL - Every day at 10:00 AM
    scheduler.add_job(
        func=run_etl_pipeline,
        args=["stock"],
        trigger=CronTrigger(hour=10, minute=0),
        id='daily_stock',
        name='Daily Stock ETL',
        replace_existing=True
    )
    
    # Weather ETL - Every 6 hours (00:00, 06:00, 12:00, 18:00)
    scheduler.add_job(
        func=run_etl_pipeline,
        args=["weather"],
        trigger=CronTrigger(hour='*/6', minute=0),
        id='weather_6hourly',
        name='Weather ETL (6 hourly)',
        replace_existing=True
    )
    
    # Forex ETL - Every day at 5:00 PM (after market close)
    scheduler.add_job(
        func=run_etl_pipeline,
        args=["forex"],
        trigger=CronTrigger(hour=17, minute=0),
        id='daily_forex',
        name='Daily Forex ETL',
        replace_existing=True
    )
    
    # FRED ETL - Every Monday at 9:00 AM (weekly, since FRED updates weekly)
    scheduler.add_job(
        func=run_etl_pipeline,
        args=["fred"],
        trigger=CronTrigger(day_of_week='mon', hour=9, minute=0),
        id='weekly_fred',
        name='Weekly FRED ETL',
        replace_existing=True
    )
    
    # Finnhub ETL - Every day at 11:00 AM
    scheduler.add_job(
        func=run_etl_pipeline,
        args=["finnhub"],
        trigger=CronTrigger(hour=11, minute=0),
        id='daily_finnhub',
        name='Daily Finnhub ETL',
        replace_existing=True
    )
    
    # All pipelines - Every day at midnight (full sync)
    scheduler.add_job(
        func=run_etl_pipeline,
        args=["all"],
        trigger=CronTrigger(hour=0, minute=0),
        id='daily_all',
        name='Daily Full ETL',
        replace_existing=True
    )
    
    # Start the scheduler
    scheduler.start()
    
    logger.info("=" * 60)
    logger.info("ETL Pipeline Scheduler Started")
    logger.info("=" * 60)
    
    # Print scheduled jobs
    for job in scheduler.get_jobs():
        logger.info(f"✓ Scheduled: {job.name}")
        logger.info(f"  ID: {job.id}")
        logger.info(f"  Trigger: {job.trigger}")
        logger.info("")
    
    logger.info("=" * 60)
    logger.info(f"Scheduler started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("Scheduler is running. Press Ctrl+C to stop.")
    logger.info("=" * 60)
    
    try:
        # This will run forever
        while True:
            pass
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
        scheduler.shutdown()
        logger.info("Scheduler shutdown complete")


if __name__ == "__main__":
    schedule_pipelines()
