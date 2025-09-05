# Scrapers/pipelines.py
from itemadapter import ItemAdapter
import os
import json
from io import BytesIO
from scrapy.exporters import CsvItemExporter
import boto3
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Prefer the project's Utf8BomCsvItemExporter if available; otherwise fallback to CsvItemExporter.
try:
    from Scrapers.exporters import Utf8BomCsvItemExporter  # custom exporter that writes BOM and handles UTF-8
except Exception:
    Utf8BomCsvItemExporter = CsvItemExporter


class YellowpagesPipeline:
    def process_item(self, item, spider):
        return item


class S3OrLocalCsvPipeline:
    """
    Pipeline that writes exported CSV to either local disk or S3 (Lightsail Object Storage).
    It also writes a JSON summary (to S3 or local).
    """
    def __init__(self, save_to_s3, s3_bucket, s3_region, output_file, summary_file, s3_key, s3_summary_key):
        self.save_to_s3 = save_to_s3
        self.s3_bucket = s3_bucket
        self.s3_region = s3_region
        self.output_file = output_file
        self.summary_file = summary_file
        self.s3_key = s3_key
        self.s3_summary_key = s3_summary_key
        self.items = []
        self.start_time = datetime.utcnow()
        self.file = None
        self.exporter = None

    @classmethod
    def from_crawler(cls, crawler):
        spider = crawler.spider

        # If the spider provided s3_bucket/s3_region via -a, use them; otherwise fall back to crawler settings.
        s3_bucket = getattr(spider, 's3_bucket', None) or crawler.settings.get('S3_BUCKET', 'bucket-euvdfl')
        s3_region = getattr(spider, 's3_region', None) or crawler.settings.get('S3_REGION', 'ca-central-1')

        save_to_s3 = getattr(spider, 'save_to_s3', False)

        # Ensure s3_key/summary_key are set on spider (they should be built in spider __init__)
        s3_key = getattr(spider, 's3_key', spider.output_file.replace(os.sep, "/"))
        s3_summary_key = getattr(spider, 's3_summary_key', spider.summary_file.replace(os.sep, "/"))

        return cls(
            save_to_s3=save_to_s3,
            s3_bucket=s3_bucket,
            s3_region=s3_region,
            output_file=spider.output_file,
            summary_file=spider.summary_file,
            s3_key=s3_key,
            s3_summary_key=s3_summary_key
        )

    def open_spider(self, spider):
        self.spider = spider
        # If uploading to S3, use a BytesIO buffer; otherwise open local file in binary mode.
        if self.save_to_s3:
            self.file = BytesIO()
            logger.info("Pipeline: writing CSV to memory buffer for S3 upload.")
        else:
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            self.file = open(self.output_file, 'wb')
            logger.info("Pipeline: writing CSV to local file: %s", self.output_file)

        # Instantiate exporter (Utf8BomCsvItemExporter preferred if available)
        # Note: CsvItemExporter expects a binary file-like object.
        self.exporter = Utf8BomCsvItemExporter(self.file)
        # If using a CsvItemExporter fallback and it's not writing BOM, that is acceptable as fallback.
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        # Export the item and also keep in-memory list for summary stats if required
        self.exporter.export_item(item)
        self.items.append(item)
        return item

    def close_spider(self, spider):
        # Finish exporter
        try:
            self.exporter.finish_exporting()
        except Exception:
            logger.exception("Exporter finish_exporting raised an exception (continuing)")

        # Upload or save CSV
        if self.save_to_s3:
            try:
                s3_client = boto3.client('s3', region_name=self.s3_region)
                body = self.file.getvalue()
                s3_client.put_object(Bucket=self.s3_bucket, Key=self.s3_key, Body=body)
                spider.logger.info(f"CSV uploaded to S3: s3://{self.s3_bucket}/{self.s3_key}")
            except Exception:
                logger.exception("Failed to upload CSV to S3")
        else:
            try:
                self.file.close()
                spider.logger.info(f"CSV saved locally to: {self.output_file}")
            except Exception:
                logger.exception("Failed to close local CSV file")

        # Build summary
        end_time = datetime.utcnow()
        elapsed = end_time - self.start_time
        unique_count = len(getattr(spider, "seen_listing_ids", []))
        duplicate_items = getattr(spider, "duplicate_items", 0)
        excluded_items = getattr(spider, "excluded_items", 0)

        summary_data = {
            "run_id": getattr(spider, "run_id", ""),
            "scraper_name": getattr(spider, "name", ""),
            "start_time_utc": self.start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time_utc": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "elapsed_seconds": int(elapsed.total_seconds()),
            "elapsed_minutes": int(elapsed.total_seconds() // 60),
            "total_encountered": unique_count + duplicate_items,
            "unique_items": unique_count,
            "duplicate_items": duplicate_items,
            "errors": getattr(spider, "errors", 0),
            "excluded_record_count": excluded_items,
            "saved_items": max(0, unique_count - excluded_items),
            "what_inputs": sorted(set(getattr(spider, "what_list", []))),
            "where_inputs": sorted(set(getattr(spider, "where_list", []))),
            "source": getattr(spider, "source", ""),
            "category_matching": "yes" if getattr(spider, "category_matching", False) else "no",
            "output_file": f"s3://{self.s3_bucket}/{self.s3_key}" if self.save_to_s3 else self.output_file,
            "summary_file": f"s3://{self.s3_bucket}/{self.s3_summary_key}" if self.save_to_s3 else self.summary_file,
            "notes": spider.crawler.stats.get_value('finish_reason', '')
        }

        # Write summary
        if self.save_to_s3:
            try:
                json_content = json.dumps(summary_data, indent=2, ensure_ascii=False).encode('utf-8')
                s3_client = boto3.client('s3', region_name=self.s3_region)
                s3_client.put_object(Bucket=self.s3_bucket, Key=self.s3_summary_key, Body=json_content)
                spider.logger.info(f"Summary uploaded to S3: s3://{self.s3_bucket}/{self.s3_summary_key}")
            except Exception:
                logger.exception("Failed to upload summary to S3")
        else:
            try:
                os.makedirs(os.path.dirname(self.summary_file), exist_ok=True)
                with open(self.summary_file, "w", encoding="utf-8") as f:
                    json.dump(summary_data, f, indent=2, ensure_ascii=False)
                spider.logger.info(f"Summary saved locally to: {self.summary_file}")
            except Exception:
                logger.exception("Failed to write local summary file")
