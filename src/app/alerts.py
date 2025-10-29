"""Alert system for state change notifications."""

from datetime import datetime


def send_alert(msg: str) -> None:
    """Send an alert message.
    
    For now, prints to console. Later can be wired to email/Slack.
    
    Args:
        msg: Alert message to send
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[ALERT {timestamp}] {msg}")

