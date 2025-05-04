"""
Simple and robust alert system for scheduler errors.

This module provides functions to notify about errors in the scheduler jobs.
Since email methods are failing, this implementation focuses on reliable
file-based alerts that can be monitored.
"""

import os
import logging
import socket
import platform
from datetime import datetime
import traceback
import smtplib
from email.message import EmailMessage


# Email configuration (change these to match your setup)
SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587
EMAIL_SENDER = "Shehata.Amr@Andalusiagroup.net"
EMAIL_PASSWORD = "13211321"
EMAIL_RECIPIENT = "Mohamed.Reda@Andalusiagroup.net"

def notify_email(subject, body):
    """Send an alert via email."""
    try:
        msg = EmailMessage()
        msg.set_content(body)
        msg["Subject"] = subject
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECIPIENT

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)

        logger.info("Email notification sent successfully.")
    except Exception as e:
        logger.error(f"Email notification failed: {str(e)}")


# Configure logging without using Unicode characters
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("alert_system.log", encoding="utf-8"),  # Specify encoding
        logging.StreamHandler()  # Console output
    ]
)

logger = logging.getLogger(__name__)

# Alert configuration
ALERT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alerts")
ALERT_HISTORY_FILE = os.path.join(ALERT_DIR, "alert_history.log")
MAX_ALERTS = 100  # Maximum number of alert files to keep

def ensure_alert_directory():
    """Make sure the alerts directory exists."""
    if not os.path.exists(ALERT_DIR):
        try:
            os.makedirs(ALERT_DIR)
            logger.info(f"Created alerts directory at {ALERT_DIR}")
        except Exception as e:
            logger.error(f"Failed to create alerts directory: {str(e)}")
            return False
    return True

def write_alert_to_file(subject, body):
    """
    Write an alert to a timestamped file in the alerts directory.
    Returns the path to the created file.
    """
    if not ensure_alert_directory():
        return None
        
    # Create a timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    alert_type = subject.replace(" ", "_").replace("/", "_").replace("\\", "_")[:30]
    filename = f"ALERT_{timestamp}_{alert_type}.txt"
    filepath = os.path.join(ALERT_DIR, filename)
    
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"SUBJECT: {subject}\n")
            f.write(f"TIME: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"HOST: {socket.gethostname()}\n")
            f.write(f"SYSTEM: {platform.system()} {platform.release()}\n")
            f.write(f"PYTHON: {platform.python_version()}\n")
            f.write("\n" + "="*50 + "\n\n")
            f.write(body)
        
        # Log alert to history file
        with open(ALERT_HISTORY_FILE, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {subject}\n")
            
        logger.info(f"Alert written to file: {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Failed to write alert to file: {str(e)}")
        return None

def cleanup_old_alerts():
    """Clean up old alert files if there are too many."""
    if not os.path.exists(ALERT_DIR):
        return
        
    try:
        # Get all alert files
        alert_files = [os.path.join(ALERT_DIR, f) for f in os.listdir(ALERT_DIR)
                     if f.startswith("ALERT_") and f.endswith(".txt")]
        
        # If we have more than MAX_ALERTS, delete the oldest ones
        if len(alert_files) > MAX_ALERTS:
            # Sort by modification time
            alert_files.sort(key=os.path.getmtime)
            
            # Delete oldest files
            for i in range(len(alert_files) - MAX_ALERTS):
                try:
                    os.remove(alert_files[i])
                    logger.info(f"Cleaned up old alert file: {alert_files[i]}")
                except:
                    pass
    except Exception as e:
        logger.error(f"Error during alert cleanup: {str(e)}")

def send_alert(subject, body):
    """
    Send an alert using file-based method.
    """
    filepath = write_alert_to_file(subject, body)
    cleanup_old_alerts()

    # Optional email notification
    notify_email(subject, body)

    return filepath is not None

def error_handler(job_name, error_info):
    """
    Handle errors by logging them and creating alert files.
    """
    # Get hostname for better error tracking
    hostname = socket.gethostname()
    
    # Format error message
    error_subject = f"Error in {job_name} on {hostname}"
    error_body = (
        f"An error occurred in the {job_name} job on {hostname} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.\n\n"
        f"Error details:\n{error_info}\n\n"
        f"Please check the scheduler.log file for more information."
    )
    
    # Log the error
    logger.error(f"{error_subject}\n{error_body}")
    
    # Send alert
    send_alert(error_subject, error_body)

def test_alert_system():
    """
    Test function to verify that the alert system is working properly.
    """
    logger.info("Testing alert system...")
    
    # Create a test alert
    test_subject = "Test Alert"
    test_body = (
        f"This is a test alert from the scheduler on {socket.gethostname()}.\n"
        f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"System: {platform.system()} {platform.release()}\n"
        f"Python: {platform.python_version()}\n\n"
        "If you see this file, the alert system is working correctly."
    )
    
    success = send_alert(test_subject, test_body)
    if success:
        logger.info("Test alert created successfully!")
        
        # Also test error handling by simulating an error
        try:
            # Simulate an error condition
            raise ValueError("This is a simulated error to test the error handling system")
        except Exception as e:
            error_info = f"{str(e)}\n\n{traceback.format_exc()}"
            error_handler("TEST_ERROR", error_info)
            logger.info("Test error alert triggered successfully!")
            
        # Show alert directory location
        logger.info(f"Alert files are stored in: {os.path.abspath(ALERT_DIR)}")
        logger.info("Please check this directory for the test alert files.")
        
        return True
    else:
        logger.error("Failed to create test alert.")
        return False

if __name__ == "__main__":
    # Run test when this module is executed directly
    test_alert_system()