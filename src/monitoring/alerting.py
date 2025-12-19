# src/monitoring/alerting.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, List
import requests
from ..utils.logger import logger


class AlertManager:
    """Alert manager for ETL pipeline failures"""
    
    def __init__(self):
        self.config = {
            "email": {
                "smtp_server": "smtp.gmail.com",
                "smtp_port": 587,
                "sender": "etl-alerts@example.com",
                "recipients": ["data-team@example.com"]
            },
            "slack": {
                "webhook_url": None  # Set your Slack webhook URL
            },
            "thresholds": {
                "error_rate": 0.05,
                "consecutive_failures": 3
            }
        }
    
    def send_alert(self, alert_type: str, message: str, details: Dict[str, Any]):
        """Send alert via configured channels"""
        alert_data = {
            "type": alert_type,
            "message": message,
            "details": details,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Log alert
        logger.error(f"ALERT: {message}", **details)
        
        # Send email alert
        self._send_email_alert(alert_data)
        
        # Send Slack alert (if configured)
        if self.config["slack"]["webhook_url"]:
            self._send_slack_alert(alert_data)
    
    def _send_email_alert(self, alert_data: Dict[str, Any]):
        """Send email alert"""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.config["email"]["sender"]
            msg['To'] = ', '.join(self.config["email"]["recipients"])
            msg['Subject'] = f"ETL Pipeline Alert: {alert_data['type']}"
            
            body = f"""
            ETL Pipeline Alert
            
            Type: {alert_data['type']}
            Message: {alert_data['message']}
            Time: {alert_data['timestamp']}
            
            Details:
            {json.dumps(alert_data['details'], indent=2)}
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            with smtplib.SMTP(
                self.config["email"]["smtp_server"],
                self.config["email"]["smtp_port"]
            ) as server:
                server.starttls()
                # Note: In production, use AWS SES or similar
                server.login(
                    self.config["email"]["sender"],
                    os.getenv("EMAIL_PASSWORD")
                )
                server.send_message(msg)
                
        except Exception as e:
            logger.error("Failed to send email alert", exc_info=e)
    
    def _send_slack_alert(self, alert_data: Dict[str, Any]):
        """Send Slack alert"""
        try:
            payload = {
                "text": f"🚨 ETL Pipeline Alert: {alert_data['message']}",
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*ETL Pipeline Alert*\n*Type:* {alert_data['type']}\n*Message:* {alert_data['message']}"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"```{json.dumps(alert_data['details'], indent=2)}```"
                        }
                    }
                ]
            }
            
            response = requests.post(
                self.config["slack"]["webhook_url"],
                json=payload
            )
            response.raise_for_status()
            
        except Exception as e:
            logger.error("Failed to send Slack alert", exc_info=e)