import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()  # load .env values from root directory

def send_failure_email(error_msg):
    sender = os.getenv("ALERT_EMAIL_FROM")
    receiver = os.getenv("ALERT_EMAIL_TO")
    password = os.getenv("ALERT_EMAIL_PASSWORD")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "❌ ETL Pipeline Failed"
    msg["From"] = sender
    msg["To"] = receiver

    html = f"""
    <html>
    <body>
        <h3>🚨 ETL Failure Alert</h3>
        <p><strong>Error:</strong> {error_msg}</p>
    </body>
    </html>
    """

    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.sendmail(sender, receiver, msg.as_string())
        logging.info("📧 Alert email sent.")
    except Exception as e:
        logging.error(f"❌ Failed to send alert email: {e}")
