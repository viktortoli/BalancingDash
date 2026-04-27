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
    items = sorted(alarms, key=lambda a: a.timestamp)
    if not items:
        return
    crit = sum(1 for a in items if a.severity == "Critical")
    warn = len(items) - crit
    subject = f"Balancing {crit}C/{warn}W"
    lines = [
        f"{a.timestamp.strftime('%H:%M')} [{a.severity[0]}] {a.message}"
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
