# Scrapers/utils/notifier.py
import os
import time
import logging
import requests
from typing import List, Optional, Tuple, IO

logger = logging.getLogger(__name__)

# Defaults will be read from environment; you can also set them from settings and pass to MailgunNotifier
DEFAULT_MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY")
DEFAULT_MAILGUN_DOMAIN = os.getenv("MAILGUN_DOMAIN")
DEFAULT_FROM = os.getenv("NOTIFY_FROM")  # e.g. "Scraper <scraper@yourdomain.com>"
DEFAULT_TO = os.getenv("NOTIFY_TO")  # comma separated: "ops@example.com,dev@example.com"

class MailgunNotifier:
    def __init__(
        self,
        api_key: Optional[str] = None,
        domain: Optional[str] = None,
        sender: Optional[str] = None,
        default_recipients: Optional[List[str]] = None,
        max_retries: int = 3,
        backoff_seconds: float = 2.0,
        timeout: int = 30,
    ):
        self.api_key = api_key or DEFAULT_MAILGUN_API_KEY
        self.domain = domain or DEFAULT_MAILGUN_DOMAIN
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds

        # Resolve sender: prefer explicit, else build from domain if available
        if sender:
            self.sender = sender
        elif self.domain:
            self.sender = f"scraper-notify@{self.domain}"
            logger.info("MailgunNotifier: no explicit sender provided — using fallback sender '%s'", self.sender)
        else:
            self.sender = None
            logger.warning("MailgunNotifier: no sender and no domain available; 'from' will be missing")

        if default_recipients is None and DEFAULT_TO:
            self.default_recipients = [e.strip() for e in DEFAULT_TO.split(",") if e.strip()]
        else:
            self.default_recipients = default_recipients or []

        if not self.api_key or not self.domain:
            logger.warning("Mailgun credentials not provided (MAILGUN_API_KEY or MAILGUN_DOMAIN). MailgunNotifier will raise on send.")

    def _post(self, data, files=None):
        url = f"https://api.mailgun.net/v3/{self.domain}/messages"
        auth = ("api", self.api_key)
        return requests.post(url, auth=auth, data=data, files=files, timeout=self.timeout)

    def send(
        self,
        subject: str,
        text: str,
        to: Optional[List[str]] = None,
        html: Optional[str] = None,
        attachments: Optional[List[Tuple[str, IO]]] = None,
    ):
        """
        Send an email via Mailgun.
        attachments: list of tuples (filename, fileobj) where fileobj is open binary file-like object.
        """
        if not self.api_key or not self.domain:
            raise RuntimeError("Mailgun API key or domain not configured")

        recipients = to or self.default_recipients
        if not recipients:
            raise RuntimeError("No recipients provided for notification")

        data = {
            "from": self.sender,
            "to": recipients,
            "subject": subject,
            "text": text,
        }
        if html:
            data["html"] = html

        files = []
        if attachments:
            for filename, fileobj in attachments:
                # requests expects files as tuples: ('attachment', (filename, fileobj))
                files.append(("attachment", (filename, fileobj)))

        attempt = 0
        while attempt < self.max_retries:
            try:
                attempt += 1
                resp = self._post(data, files=files if files else None)
                if resp.status_code >= 200 and resp.status_code < 300:
                    logger.info("MailgunNotifier: email sent successfully (status=%s)", resp.status_code)
                    return resp.json()
                else:
                    logger.warning(
                        "MailgunNotifier: attempt %d failed (status=%s): %s", attempt, resp.status_code, resp.text
                    )
            except Exception as e:
                logger.exception("MailgunNotifier: exception on attempt %d: %s", attempt, e)

            time.sleep(self.backoff_seconds * attempt)

        raise RuntimeError("MailgunNotifier: all attempts to send email failed")
