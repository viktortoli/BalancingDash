from __future__ import annotations

import smtplib
from email.message import EmailMessage
from typing import Iterable

from alarms import Alarm


def send(
    alarms: Iterable[Alarm],
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    sender: str,
    recipients: list[str],
) -> None:
    items = list(alarms)
    if not items:
        return
    crit = [a for a in items if a.severity == "Critical"]
    warn = [a for a in items if a.severity == "Warning"]
    subject = f"[Balancing] {len(crit)} critical, {len(warn)} warning"
    lines = [
        f"[{a.severity}] {a.timestamp.strftime('%Y-%m-%d %H:%M %Z')}  {a.code}: {a.message}"
        for a in items
    ]
    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.set_content("\n".join(lines))

    with smtplib.SMTP(host, port, timeout=15) as s:
        s.starttls()
        s.login(user, password)
        s.send_message(msg)
