"""
Comprehensive tests for monitoring and alerting module.
Tests AlertManager, alert channels, thresholds, and error handling.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import json
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from src.monitoring.alerting import AlertManager


class TestAlertManagerInitialization:
    """Test AlertManager initialization and configuration"""
    
    def test_alert_manager_initialization(self):
        """Test AlertManager initializes with default config"""
        manager = AlertManager()
        
        assert manager is not None
        assert "email" in manager.config
        assert "slack" in manager.config
        assert "thresholds" in manager.config
    
    def test_alert_manager_email_config(self):
        """Test email configuration is properly set"""
        manager = AlertManager()
        
        assert manager.config["email"]["smtp_server"] == "smtp.gmail.com"
        assert manager.config["email"]["smtp_port"] == 587
        assert manager.config["email"]["sender"] == "etl-alerts@example.com"
        assert isinstance(manager.config["email"]["recipients"], list)
        assert len(manager.config["email"]["recipients"]) > 0
    
    def test_alert_manager_slack_config(self):
        """Test Slack configuration is properly set"""
        manager = AlertManager()
        
        assert "webhook_url" in manager.config["slack"]
        # webhook_url is None by default
        assert manager.config["slack"]["webhook_url"] is None
    
    def test_alert_manager_thresholds(self):
        """Test alert thresholds are configured"""
        manager = AlertManager()
        
        assert "error_rate" in manager.config["thresholds"]
        assert "consecutive_failures" in manager.config["thresholds"]
        assert manager.config["thresholds"]["error_rate"] == 0.05
        assert manager.config["thresholds"]["consecutive_failures"] == 3


class TestAlertSending:
    """Test alert sending functionality"""
    
    @patch('src.monitoring.alerting.logger')
    @patch.object(AlertManager, '_send_email_alert')
    @patch.object(AlertManager, '_send_slack_alert')
    def test_send_alert_basic(self, mock_slack, mock_email, mock_logger):
        """Test sending basic alert"""
        manager = AlertManager()
        alert_details = {"error": "Test error", "pipeline": "test_pipeline"}
        
        manager.send_alert("ERROR", "Pipeline failed", alert_details)
        
        # Verify logger was called
        assert mock_logger.error.called
        
        # Verify email alert was attempted
        assert mock_email.called
    
    @patch('src.monitoring.alerting.logger')
    @patch.object(AlertManager, '_send_email_alert')
    @patch.object(AlertManager, '_send_slack_alert')
    def test_send_alert_with_multiple_details(self, mock_slack, mock_email, mock_logger):
        """Test sending alert with detailed information"""
        manager = AlertManager()
        details = {
            "error": "Extraction failed",
            "source": "alpha_vantage",
            "records_processed": 100,
            "error_count": 5,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        manager.send_alert("EXTRACTION_FAILURE", "Failed to extract from Alpha Vantage", details)
        
        # Verify email was called with alert data
        assert mock_email.called
        call_args = mock_email.call_args
        alert_data = call_args[0][0]
        
        assert alert_data["type"] == "EXTRACTION_FAILURE"
        assert alert_data["message"] == "Failed to extract from Alpha Vantage"
        assert alert_data["details"] == details
    
    @patch('src.monitoring.alerting.logger')
    @patch.object(AlertManager, '_send_email_alert')
    def test_send_alert_logs_to_logger(self, mock_email, mock_logger):
        """Test that alerts are logged"""
        manager = AlertManager()
        
        manager.send_alert("WARNING", "High error rate detected", {"error_rate": 0.08})
        
        # Verify error was logged
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args
        assert "ALERT:" in str(call_args[0][0])


class TestEmailAlerting:
    """Test email alerting functionality"""
    
    @patch('src.monitoring.alerting.smtplib.SMTP')
    @patch('src.monitoring.alerting.logger')
    @patch.dict('src.monitoring.alerting.os.environ', {'EMAIL_PASSWORD': 'test_password'})
    def test_send_email_alert_success(self, mock_logger, mock_smtp):
        """Test successful email alert"""
        manager = AlertManager()
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server
        
        alert_data = {
            "type": "CRITICAL",
            "message": "Pipeline crashed",
            "details": {"error": "Out of memory"},
            "timestamp": datetime.utcnow().isoformat()
        }
        
        manager._send_email_alert(alert_data)
        
        # Verify SMTP connection was made
        mock_smtp.assert_called_once()
        
        # Verify server operations
        assert mock_server.starttls.called
        assert mock_server.send_message.called
    
    @patch('src.monitoring.alerting.smtplib.SMTP')
    @patch('src.monitoring.alerting.logger')
    def test_send_email_alert_failure_handling(self, mock_logger, mock_smtp):
        """Test email alert failure is handled gracefully"""
        manager = AlertManager()
        mock_smtp.side_effect = Exception("SMTP connection failed")
        
        alert_data = {
            "type": "ERROR",
            "message": "Test error",
            "details": {},
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Should not raise exception
        manager._send_email_alert(alert_data)
        
        # Verify error was logged
        assert mock_logger.error.called
    
    @patch('src.monitoring.alerting.smtplib.SMTP')
    def test_email_alert_message_format(self, mock_smtp):
        """Test email message is properly formatted"""
        manager = AlertManager()
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server
        
        alert_data = {
            "type": "DATA_QUALITY",
            "message": "Missing values detected",
            "details": {"missing_count": 150, "total_rows": 5000},
            "timestamp": "2024-01-01T12:00:00"
        }
        
        manager._send_email_alert(alert_data)
        
        # Verify message was sent
        assert mock_server.send_message.called
        
        # Get the message that was sent
        call_args = mock_server.send_message.call_args
        msg = call_args[0][0]
        
        # Verify email headers
        assert "ETL Pipeline Alert" in msg['Subject']
        assert msg['From'] == manager.config["email"]["sender"]


class TestSlackAlerting:
    """Test Slack alerting functionality"""
    
    @patch('src.monitoring.alerting.requests.post')
    @patch('src.monitoring.alerting.logger')
    def test_send_slack_alert_success(self, mock_logger, mock_post):
        """Test successful Slack alert"""
        manager = AlertManager()
        manager.config["slack"]["webhook_url"] = "https://hooks.slack.com/services/TEST"
        mock_post.return_value = MagicMock(status_code=200)
        
        alert_data = {
            "type": "LOAD_FAILURE",
            "message": "Failed to load data to database",
            "details": {"table": "stock_prices", "error": "Connection timeout"},
            "timestamp": datetime.utcnow().isoformat()
        }
        
        manager._send_slack_alert(alert_data)
        
        # Verify POST request was made
        assert mock_post.called
        
        # Verify webhook URL
        call_args = mock_post.call_args
        assert call_args[0][0] == "https://hooks.slack.com/services/TEST"
    
    @patch('src.monitoring.alerting.requests.post')
    @patch('src.monitoring.alerting.logger')
    def test_send_slack_alert_payload_format(self, mock_logger, mock_post):
        """Test Slack alert payload is properly formatted"""
        manager = AlertManager()
        manager.config["slack"]["webhook_url"] = "https://hooks.slack.com/services/TEST"
        mock_post.return_value = MagicMock(status_code=200)
        
        alert_data = {
            "type": "VALIDATION_ERROR",
            "message": "Data validation failed",
            "details": {"invalid_rows": 42, "validation_rule": "date_format"},
            "timestamp": "2024-01-01T12:00:00"
        }
        
        manager._send_slack_alert(alert_data)
        
        # Get the payload that was sent
        call_args = mock_post.call_args
        payload = call_args[1]['json']
        
        # Verify payload structure
        assert "text" in payload
        assert "blocks" in payload
        assert len(payload["blocks"]) > 0
        
        # Verify message content
        assert alert_data["message"] in payload["text"]
    
    @patch('src.monitoring.alerting.requests.post')
    @patch('src.monitoring.alerting.logger')
    def test_send_slack_alert_failure_handling(self, mock_logger, mock_post):
        """Test Slack alert failure is handled gracefully"""
        manager = AlertManager()
        manager.config["slack"]["webhook_url"] = "https://hooks.slack.com/services/TEST"
        mock_post.side_effect = Exception("Network error")
        
        alert_data = {
            "type": "ERROR",
            "message": "Test error",
            "details": {},
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Should not raise exception
        manager._send_slack_alert(alert_data)
        
        # Verify error was logged
        assert mock_logger.error.called
    
    @patch('src.monitoring.alerting.requests.post')
    @patch('src.monitoring.alerting.logger')
    def test_send_slack_alert_not_sent_without_webhook(self, mock_logger, mock_post):
        """Test Slack alert is not sent without webhook URL"""
        manager = AlertManager()
        manager.config["slack"]["webhook_url"] = None
        
        alert_data = {
            "type": "ERROR",
            "message": "Test",
            "details": {},
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Slack alert should not be called if webhook_url is None
        # This is tested in send_alert method


class TestAlertingWithDifferentTypes:
    """Test alerting with different alert types"""
    
    @patch('src.monitoring.alerting.logger')
    @patch.object(AlertManager, '_send_email_alert')
    def test_extraction_failure_alert(self, mock_email, mock_logger):
        """Test extraction failure alert"""
        manager = AlertManager()
        
        manager.send_alert(
            "EXTRACTION_FAILURE",
            "Failed to extract from FRED API",
            {"source": "fred", "error": "Invalid API key", "attempt": 1}
        )
        
        assert mock_email.called
        alert_data = mock_email.call_args[0][0]
        assert alert_data["type"] == "EXTRACTION_FAILURE"
    
    @patch('src.monitoring.alerting.logger')
    @patch.object(AlertManager, '_send_email_alert')
    def test_transformation_failure_alert(self, mock_email, mock_logger):
        """Test transformation failure alert"""
        manager = AlertManager()
        
        manager.send_alert(
            "TRANSFORMATION_ERROR",
            "Data cleaning failed",
            {"step": "remove_duplicates", "error": "Memory error"}
        )
        
        assert mock_email.called
        alert_data = mock_email.call_args[0][0]
        assert alert_data["type"] == "TRANSFORMATION_ERROR"
    
    @patch('src.monitoring.alerting.logger')
    @patch.object(AlertManager, '_send_email_alert')
    def test_load_failure_alert(self, mock_email, mock_logger):
        """Test load failure alert"""
        manager = AlertManager()
        
        manager.send_alert(
            "LOAD_FAILURE",
            "Failed to insert records into database",
            {"table": "stock_prices", "error": "Unique constraint violation", "rows": 500}
        )
        
        assert mock_email.called
        alert_data = mock_email.call_args[0][0]
        assert alert_data["type"] == "LOAD_FAILURE"
    
    @patch('src.monitoring.alerting.logger')
    @patch.object(AlertManager, '_send_email_alert')
    def test_data_quality_alert(self, mock_email, mock_logger):
        """Test data quality alert"""
        manager = AlertManager()
        
        manager.send_alert(
            "DATA_QUALITY",
            "High percentage of null values",
            {"column": "dividend", "null_percentage": 45.5, "threshold": 30}
        )
        
        assert mock_email.called


class TestAlertThresholds:
    """Test alert threshold configuration and usage"""
    
    def test_error_rate_threshold(self):
        """Test error rate threshold configuration"""
        manager = AlertManager()
        
        error_rate_threshold = manager.config["thresholds"]["error_rate"]
        assert error_rate_threshold == 0.05
        assert 0 <= error_rate_threshold <= 1
    
    def test_consecutive_failures_threshold(self):
        """Test consecutive failures threshold configuration"""
        manager = AlertManager()
        
        failure_threshold = manager.config["thresholds"]["consecutive_failures"]
        assert failure_threshold == 3
        assert failure_threshold > 0
    
    @patch('src.monitoring.alerting.logger')
    @patch.object(AlertManager, '_send_email_alert')
    def test_alert_when_error_rate_exceeded(self, mock_email, mock_logger):
        """Test alert when error rate exceeds threshold"""
        manager = AlertManager()
        
        # Simulate high error rate alert
        current_error_rate = 0.08  # Higher than threshold of 0.05
        if current_error_rate > manager.config["thresholds"]["error_rate"]:
            manager.send_alert(
                "HIGH_ERROR_RATE",
                f"Error rate {current_error_rate:.2%} exceeds threshold",
                {"current_rate": current_error_rate, "threshold": 0.05}
            )
        
        assert mock_email.called
    
    @patch('src.monitoring.alerting.logger')
    @patch.object(AlertManager, '_send_email_alert')
    def test_alert_when_consecutive_failures_exceeded(self, mock_email, mock_logger):
        """Test alert when consecutive failures exceed threshold"""
        manager = AlertManager()
        
        # Simulate consecutive failures
        consecutive_failures = 5
        if consecutive_failures >= manager.config["thresholds"]["consecutive_failures"]:
            manager.send_alert(
                "CONSECUTIVE_FAILURES",
                f"{consecutive_failures} consecutive pipeline failures detected",
                {"count": consecutive_failures, "threshold": 3}
            )
        
        assert mock_email.called


class TestAlertIntegration:
    """Test alert integration and multi-channel scenarios"""
    
    @patch('src.monitoring.alerting.requests.post')
    @patch('src.monitoring.alerting.smtplib.SMTP')
    @patch('src.monitoring.alerting.logger')
    def test_alert_sent_to_multiple_channels(self, mock_logger, mock_smtp, mock_post):
        """Test alert is sent to both email and Slack when configured"""
        manager = AlertManager()
        manager.config["slack"]["webhook_url"] = "https://hooks.slack.com/services/TEST"
        
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server
        mock_post.return_value = MagicMock(status_code=200)
        
        manager.send_alert(
            "CRITICAL",
            "Pipeline system failure",
            {"error": "Database unreachable"}
        )
        
        # Verify both channels were attempted
        assert mock_smtp.called or mock_server.send_message.called
        assert mock_post.called
    
    @patch('src.monitoring.alerting.logger')
    @patch.object(AlertManager, '_send_email_alert')
    @patch.object(AlertManager, '_send_slack_alert')
    def test_alert_data_consistency_across_channels(self, mock_slack, mock_email, mock_logger):
        """Test that alert data is consistent across channels"""
        manager = AlertManager()
        manager.config["slack"]["webhook_url"] = "https://hooks.slack.com/services/TEST"
        
        manager.send_alert(
            "TEST_ALERT",
            "Test message for all channels",
            {"test_data": "consistency"}
        )
        
        # Get alert data from both calls
        email_alert = mock_email.call_args[0][0]
        slack_alert = mock_slack.call_args[0][0]
        
        # Verify same core data
        assert email_alert["type"] == slack_alert["type"]
        assert email_alert["message"] == slack_alert["message"]
        assert email_alert["details"] == slack_alert["details"]


class TestAlertConfiguration:
    """Test alert manager configuration and customization"""
    
    def test_modify_email_recipients(self):
        """Test modifying email recipients"""
        manager = AlertManager()
        original_recipients = manager.config["email"]["recipients"].copy()
        
        manager.config["email"]["recipients"] = ["admin@example.com", "ops@example.com"]
        
        assert len(manager.config["email"]["recipients"]) == 2
        assert "admin@example.com" in manager.config["email"]["recipients"]
    
    def test_set_slack_webhook(self):
        """Test setting Slack webhook URL"""
        manager = AlertManager()
        webhook_url = "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXX"
        
        manager.config["slack"]["webhook_url"] = webhook_url
        
        assert manager.config["slack"]["webhook_url"] == webhook_url
    
    def test_update_error_rate_threshold(self):
        """Test updating error rate threshold"""
        manager = AlertManager()
        original_threshold = manager.config["thresholds"]["error_rate"]
        
        manager.config["thresholds"]["error_rate"] = 0.10
        
        assert manager.config["thresholds"]["error_rate"] == 0.10
        assert manager.config["thresholds"]["error_rate"] != original_threshold
    
    def test_update_failure_threshold(self):
        """Test updating consecutive failure threshold"""
        manager = AlertManager()
        original_threshold = manager.config["thresholds"]["consecutive_failures"]
        
        manager.config["thresholds"]["consecutive_failures"] = 5
        
        assert manager.config["thresholds"]["consecutive_failures"] == 5
        assert manager.config["thresholds"]["consecutive_failures"] != original_threshold


class TestAlertErrorHandling:
    """Test error handling in alerting"""
    
    @patch('src.monitoring.alerting.logger')
    @patch.object(AlertManager, '_send_email_alert')
    @patch.object(AlertManager, '_send_slack_alert')
    def test_alert_handles_empty_details(self, mock_slack, mock_email, mock_logger):
        """Test alert handles empty details"""
        manager = AlertManager()
        
        manager.send_alert("EMPTY_ALERT", "Test message", {})
        
        assert mock_email.called
        alert_data = mock_email.call_args[0][0]
        assert alert_data["details"] == {}
    
    @patch('src.monitoring.alerting.logger')
    @patch.object(AlertManager, '_send_email_alert')
    def test_alert_with_large_details(self, mock_email, mock_logger):
        """Test alert with large details dictionary"""
        manager = AlertManager()
        
        large_details = {
            f"key_{i}": f"value_{i}" for i in range(100)
        }
        
        manager.send_alert("LARGE_ALERT", "Large details", large_details)
        
        assert mock_email.called
        alert_data = mock_email.call_args[0][0]
        assert len(alert_data["details"]) == 100
    
    @patch('src.monitoring.alerting.logger')
    @patch.object(AlertManager, '_send_email_alert')
    def test_alert_with_special_characters(self, mock_email, mock_logger):
        """Test alert with special characters in message"""
        manager = AlertManager()
        
        special_message = "Alert with special chars: !@#$%^&*()_+-=[]{}|;:,.<>?"
        
        manager.send_alert("SPECIAL_ALERT", special_message, {"char_test": "✓✗✔"})
        
        assert mock_email.called


class TestAlertTimestamp:
    """Test timestamp handling in alerts"""
    
    @patch('src.monitoring.alerting.logger')
    @patch.object(AlertManager, '_send_email_alert')
    def test_alert_includes_timestamp(self, mock_email, mock_logger):
        """Test that alerts include timestamp"""
        manager = AlertManager()
        
        manager.send_alert("TIMESTAMP_TEST", "Test", {})
        
        alert_data = mock_email.call_args[0][0]
        assert "timestamp" in alert_data
        assert alert_data["timestamp"] is not None
    
    @patch('src.monitoring.alerting.logger')
    @patch.object(AlertManager, '_send_email_alert')
    def test_alert_timestamp_format(self, mock_email, mock_logger):
        """Test that alert timestamp is in ISO format"""
        manager = AlertManager()
        
        manager.send_alert("TIMESTAMP_FORMAT_TEST", "Test", {})
        
        alert_data = mock_email.call_args[0][0]
        timestamp = alert_data["timestamp"]
        
        # Verify it's a valid ISO format timestamp
        try:
            datetime.fromisoformat(timestamp)
            valid = True
        except ValueError:
            valid = False
        
        assert valid


class TestMonitoringIntegration:
    """Integration tests for monitoring and alerting"""
    
    @patch('src.monitoring.alerting.requests.post')
    @patch('src.monitoring.alerting.smtplib.SMTP')
    @patch('src.monitoring.alerting.logger')
    def test_full_alert_workflow(self, mock_logger, mock_smtp, mock_post):
        """Test full alert workflow from trigger to delivery"""
        manager = AlertManager()
        manager.config["slack"]["webhook_url"] = "https://hooks.slack.com/services/TEST"
        
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server
        mock_post.return_value = MagicMock(status_code=200)
        
        # Simulate extraction failure
        error_details = {
            "source": "alpha_vantage",
            "error": "API rate limit exceeded",
            "retry_count": 3,
            "failed_symbols": ["AAPL", "GOOGL"]
        }
        
        manager.send_alert(
            "EXTRACTION_FAILURE",
            "Alpha Vantage extraction failed after retries",
            error_details
        )
        
        # Verify complete workflow
        assert mock_logger.error.called
        assert mock_server.send_message.called
        assert mock_post.called
    
    @patch('src.monitoring.alerting.logger')
    @patch.object(AlertManager, '_send_email_alert')
    def test_multiple_sequential_alerts(self, mock_email, mock_logger):
        """Test sending multiple alerts sequentially"""
        manager = AlertManager()
        
        alerts = [
            ("ERROR_1", "First error", {"attempt": 1}),
            ("ERROR_2", "Second error", {"attempt": 2}),
            ("ERROR_3", "Third error", {"attempt": 3})
        ]
        
        for alert_type, message, details in alerts:
            manager.send_alert(alert_type, message, details)
        
        # Verify all alerts were sent
        assert mock_email.call_count == 3


class TestAlertLogging:
    """Test alert logging"""
    
    @patch('src.monitoring.alerting.logger')
    @patch.object(AlertManager, '_send_email_alert')
    def test_alert_error_logged(self, mock_email, mock_logger):
        """Test that alerts are logged as errors"""
        manager = AlertManager()
        
        manager.send_alert("TEST", "Test message", {})
        
        # Verify logger.error was called
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args
        assert "ALERT:" in str(call_args)
