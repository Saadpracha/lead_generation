# Scrapers/pipelines.py
from itemadapter import ItemAdapter
import os
from io import BytesIO
from scrapy.exporters import CsvItemExporter
import boto3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Prefer Utf8BomCsvItemExporter if available
try:
    from Scrapers.exporters import Utf8BomCsvItemExporter
except Exception:
    Utf8BomCsvItemExporter = None

from Scrapers.utils.notifier import MailgunNotifier

class YellowpagesPipeline:
    def process_item(self, item, spider):
        return item


class S3OrLocalCsvPipeline:
    """
    Writes CSV to local disk or S3, then sends a Mailgun notification only if recipients provided.
    Recipients priority:
      1) spider.notify_emails (passed via -a notify_emails="a@x.com,b@y.com")
      2) settings.NOTIFY_EMAILS (optional parameter file or env via settings.py)
      3) notifier.default_recipients (from env used by MailgunNotifier) -- treated same as settings fallback

    If none are present -> no notification is sent (default behaviour).
    """
    def __init__(self, save_to_s3, s3_bucket, s3_region, output_file, s3_key, mailgun_conf):
        self.save_to_s3 = save_to_s3
        self.s3_bucket = s3_bucket
        self.s3_region = s3_region
        self.output_file = output_file
        self.s3_key = s3_key
        self.file = None
        self.exporter = None
        self.mailgun_conf = mailgun_conf or {}
        self.notifier = MailgunNotifier(
            api_key=self.mailgun_conf.get("api_key"),
            domain=self.mailgun_conf.get("domain"),
            sender=self.mailgun_conf.get("sender"),
            default_recipients=self.mailgun_conf.get("recipients"),
        )

    @classmethod
    def from_crawler(cls, crawler):
        spider = getattr(crawler, "spider", None)

        s3_bucket = getattr(spider, "s3_bucket", None) or crawler.settings.get("S3_BUCKET", "bucket-euvdfl")
        s3_region = getattr(spider, "s3_region", None) or crawler.settings.get("S3_REGION", "ca-central-1")
        save_to_s3 = getattr(spider, "save_to_s3", False) or crawler.settings.get("SAVE_TO_S3", False)
        s3_key = getattr(spider, "s3_key", None) or crawler.settings.get("S3_KEY")
        output_file = getattr(spider, "output_file", crawler.settings.get("OUTPUT_FILE"))

        mailgun_api_key = crawler.settings.get("MAILGUN_API_KEY")
        mailgun_domain = crawler.settings.get("MAILGUN_DOMAIN")
        mailgun_sender = crawler.settings.get("MAILGUN_FROM")
        notify_emails = crawler.settings.get("NOTIFY_EMAILS")  # list or comma-separated

        if isinstance(notify_emails, str):
            notify_list = [e.strip() for e in notify_emails.split(",") if e.strip()]
        else:
            notify_list = notify_emails or []

        mailgun_conf = {
            "api_key": mailgun_api_key,
            "domain": mailgun_domain,
            "sender": mailgun_sender,
            "recipients": notify_list,
        }

        return cls(
            save_to_s3=bool(save_to_s3),
            s3_bucket=s3_bucket,
            s3_region=s3_region,
            output_file=output_file,
            s3_key=s3_key or (output_file.replace(os.sep, "/") if output_file else None),
            mailgun_conf=mailgun_conf,
        )

    def open_spider(self, spider):
        if self.save_to_s3:
            self.file = BytesIO()
            logger.info("Pipeline: writing CSV to memory buffer for S3 upload.")
        else:
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            self.file = open(self.output_file, "wb")
            logger.info("Pipeline: writing CSV to local file: %s", self.output_file)

        if Utf8BomCsvItemExporter:
            self.exporter = Utf8BomCsvItemExporter(self.file, encoding="utf-8-sig")
        else:
            self.exporter = CsvItemExporter(self.file, encoding="utf-8-sig")

        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def _determine_recipients(self, spider):
        """
        Determine recipients. Priority:
         1) spider.notify_emails (string comma-separated or list)
         2) settings NOTIFY_EMAILS (already passed into notifier.default_recipients)
         3) notifier.default_recipients (from env)
        Return: list of emails (or empty list)
        """
        recipients = []

        # 1) spider argument (highest priority)
        notify_attr = getattr(spider, "notify_emails", None) or getattr(spider, "notify_to", None)
        if notify_attr:
            if isinstance(notify_attr, str):
                recipients = [e.strip() for e in notify_attr.split(",") if e.strip()]
            elif isinstance(notify_attr, (list, tuple)):
                recipients = [e.strip() for e in notify_attr if isinstance(e, str) and e.strip()]
            return recipients

        # 2) settings / notifier default (passed via from_crawler -> MailgunNotifier)
        if self.notifier and getattr(self.notifier, "default_recipients", None):
            return [e.strip() for e in self.notifier.default_recipients if e.strip()]

        # no recipients
        return []

    def close_spider(self, spider):
        # finish exporting
        try:
            self.exporter.finish_exporting()
        except Exception:
            logger.exception("Exporter finish_exporting raised an exception")

        upload_success = False
        upload_reason = None
        public_output_url = None
        public_summary_url = None

        # upload or close local
        if self.save_to_s3:
            try:
                s3_client = boto3.client("s3", region_name=self.s3_region)
                body = self.file.getvalue()
                s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=self.s3_key,
                    Body=body,
                    ACL="public-read",
                    ContentType="text/csv; charset=utf-8",
                )
                spider.logger.info("CSV uploaded to S3: s3://%s/%s", self.s3_bucket, self.s3_key)
                upload_success = True
                public_output_url = f"https://{self.s3_bucket}.s3.{self.s3_region}.amazonaws.com/{self.s3_key}"
                s3_summary_key = getattr(spider, "s3_summary_key", None)
                if s3_summary_key:
                    public_summary_url = f"https://{self.s3_bucket}.s3.{self.s3_region}.amazonaws.com/{s3_summary_key}"
            except Exception as e:
                logger.exception("Failed to upload CSV to S3")
                upload_reason = str(e)
        else:
            try:
                self.file.close()
                spider.logger.info("CSV saved locally to: %s", self.output_file)
                upload_success = True
            except Exception:
                logger.exception("Failed to close local CSV file")
                upload_reason = "local file close failed"

        # Build email content per client spec
        run_id = getattr(spider, "run_id", None) or f"{spider.name}-{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
        subject = run_id
        scraper_name = getattr(spider, "name", "")
        source = getattr(spider, "source", "")
        output_url = public_output_url or getattr(spider, "output_file", self.output_file) or self.output_file
        summary_url = public_summary_url or getattr(spider, "summary_file", None) or ""

        body_lines = [
            "File execution completed:",
            f"run_id: {run_id}",
            f"name: {scraper_name}",
            f"source: {source}",
            f"output_url: {output_url}",
            f"summary_url: {summary_url or ''}",
        ]
        body_text = "\n".join(body_lines)

        # attachments for local runs
        attachments = None

        # Determine recipients (respect priority)
        recipients = self._determine_recipients(spider)

        if not recipients:
            spider.logger.info("Notifications disabled: no recipients provided (pass -a notify_emails or set NOTIFY_EMAILS).")
        else:
            # Send notification
            try:
                self.notifier.send(subject=subject, text=body_text, to=recipients, attachments=attachments)
                spider.logger.info("Notification sent successfully to: %s", recipients)
            except Exception:
                logger.exception("Notification failed")

        # close attachments if opened
        if attachments:
            for _name, fileobj in attachments:
                try:
                    fileobj.close()
                except Exception:
                    pass

        spider.logger.info("Pipeline finished: upload_success=%s, reason=%s", upload_success, upload_reason)
