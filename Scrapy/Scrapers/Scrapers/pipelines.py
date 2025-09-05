from itemadapter import ItemAdapter
import os
import json
from io import BytesIO
from scrapy.exporters import CsvItemExporter
import boto3
from datetime import datetime

class YellowpagesPipeline:
    def process_item(self, item, spider):
        return item

class S3OrLocalCsvPipeline:
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

    @classmethod
    def from_crawler(cls, crawler):
        spider = crawler.spider
        return cls(
            save_to_s3=getattr(spider, 'save_to_s3', False),
            s3_bucket=getattr(spider, 's3_bucket', crawler.settings.get('S3_BUCKET', 'bucket-euvdfl')),
            s3_region=getattr(spider, 's3_region', crawler.settings.get('S3_REGION', 'ca-central-1')),
            output_file=spider.output_file,
            summary_file=spider.summary_file,
            s3_key=spider.s3_key,
            s3_summary_key=spider.s3_summary_key
        )

    def open_spider(self, spider):
        self.spider = spider
        if self.save_to_s3:
            self.file = BytesIO()
        else:
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            self.file = open(self.output_file, 'wb')
        self.exporter = Utf8BomCsvItemExporter(self.file)
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        self.items.append(item)
        return item

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        if self.save_to_s3:
            s3_client = boto3.client('s3', region_name=self.s3_region)
            body = self.file.getvalue()
            s3_client.put_object(Bucket=self.s3_bucket, Key=self.s3_key, Body=body)
            spider.logger.info(f"CSV uploaded to S3: s3://{self.s3_bucket}/{self.s3_key}")
        else:
            self.file.close()
            spider.logger.info(f"CSV saved locally to: {self.output_file}")

        end_time = datetime.utcnow()
        elapsed = end_time - self.start_time
        unique_count = len(spider.seen_listing_ids)
        total_encountered = unique_count + spider.duplicate_items
        original_items = total_encountered // 3
        duplicate_items = original_items - unique_count

        summary_data = {
            "run_id": spider.run_id,
            "scraper_name": spider.name,
            "start_time_utc": self.start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time_utc": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "elapsed_seconds": int(elapsed.total_seconds()),
            "elapsed_minutes": int(elapsed.total_seconds() // 60),
            "total_encountered": original_items,
            "unique_items": unique_count,
            "duplicate_items": duplicate_items,
            "errors": spider.errors,
            "excluded_record_count": spider.excluded_items,
            "saved_items": unique_count - spider.excluded_items,
            "what_inputs": sorted(set(spider.what_list)),
            "where_inputs": sorted(set(spider.where_list)),
            "source": spider.source,
            "category_matching": "yes" if spider.category_matching else "no",
            "output_file": f"s3://{self.s3_bucket}/{self.s3_key}" if self.save_to_s3 else self.output_file,
            "summary_file": f"s3://{self.s3_bucket}/{self.s3_summary_key}" if self.save_to_s3 else self.summary_file,
            "notes": spider.crawler.stats.get_value('finish_reason', '')
        }

        if self.save_to_s3:
            json_content = json.dumps(summary_data, indent=2, ensure_ascii=False).encode('utf-8')
            s3_client.put_object(Bucket=self.s3_bucket, Key=self.s3_summary_key, Body=json_content)
            spider.logger.info(f"Summary uploaded to S3: s3://{self.s3_bucket}/{self.s3_summary_key}")
        else:
            os.makedirs(os.path.dirname(self.summary_file), exist_ok=True)
            with open(self.summary_file, "w", encoding="utf-8") as f:
                json.dump(summary_data, f, indent=2, ensure_ascii=False)
            spider.logger.info(f"Summary saved locally to: {self.summary_file}")