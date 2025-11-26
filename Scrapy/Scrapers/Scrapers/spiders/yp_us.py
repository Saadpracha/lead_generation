import scrapy
import os
import json
import base64
import pandas as pd
import re
import unicodedata
from urllib.parse import urljoin
from datetime import datetime
from itertools import product
import boto3


class YellowPagesUsSpider(scrapy.Spider):
    name = "yellowpages_us"

    base_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 Edg/139.0.0.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Sec-Ch-Ua": '"Not;A=Brand";v="99", "Microsoft Edge";v="139", "Chromium";v="139"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Referer": "https://www.yellowpages.com/",
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.crawler = crawler

        crawler.settings.set('FEEDS', {}, priority='spider')
        crawler.settings.set('FEED_EXPORT_ENABLED', False, priority='spider')

        spider.s3_bucket = crawler.settings.get("S3_BUCKET")
        spider.s3_region = crawler.settings.get("S3_REGION")

        if kwargs.get("save_to_s3", "no").lower() == "yes":
            spider.save_to_s3 = True
            if not spider.s3_bucket or not spider.s3_region:
                raise ValueError("Missing S3_BUCKET or S3_REGION in settings.py or spider arguments.")
        else:
            spider.save_to_s3 = False

        if spider.save_to_s3:
            spider.logger.info(f"Output will be saved to S3: s3://{spider.s3_bucket}/{spider.s3_key}")
        else:
            spider.logger.info(f"Output file will be saved locally to: {spider.output_file}")

        return spider
    
    def __init__(self, what=None, where=None, dir_name=None, summary=None, source=None, category_matching="no", output_file="output.csv", *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not what or not where:
            raise ValueError("Please provide both -a what=... and -a where=...")
        
        if not dir_name:
            raise ValueError("Please provide -a dir_name=...")
        
        if not output_file.endswith('.csv'):
            raise ValueError("Output file must have a .csv extension")

        # Validate output file name for invalid characters
        if not re.match(r'^[\w\-\.]+$', output_file):
            raise ValueError("Output file name contains invalid characters")

        # Define output directory and file paths
        self.output_dir = os.path.join("imp_data", "YP_US", "output", dir_name)
        try:
            os.makedirs(self.output_dir, exist_ok=True)
        except OSError as e:
            raise ValueError(f"Failed to create output directory '{self.output_dir}': {e}")

        self.output_file = os.path.join(self.output_dir, output_file)
        self.summary_file = os.path.join(self.output_dir, summary or "summary.json")

        # S3 flags & preliminary values (do NOT access crawler.settings here)
        self.save_to_s3 = kwargs.get("save_to_s3", "no").lower() == "yes"
        # Accept s3_bucket/s3_region from CLI if provided; pipeline will fill defaults if None
        self.s3_key = self.output_file.replace(os.sep, "/")
        self.s3_summary_key = self.summary_file.replace(os.sep, "/")

        self.root_dir = os.getcwd()
        self.proxy_list = self.load_proxies(os.path.join(self.root_dir, "proxies.json"))
        self.current_proxy_index = 0

        self.whats = self.read_excel_column(os.path.join(self.root_dir, what), "what")
        self.wheres = self.read_excel_column(os.path.join(self.root_dir, where), "where")
        self.logger.info(f"Loaded {len(self.whats)} 'what' terms and {len(self.wheres)} 'where' terms")

        self.category_matching = category_matching.strip().lower() == "yes"
        self.seen_ids = set()
        self.duplicate_count = 0
        self.excluded_records = 0
        self.errors = 0
        self.total_responses = 0
        self.source = source or ""

        self.start_time = datetime.utcnow()
        self.run_id = f"{self.name}-{self.start_time.strftime('%Y%m%dT%H%M%SZ')}"


    def load_proxies(self, filepath):
        try:
            with open(filepath, 'r', encoding="utf-8-sig") as f:
                proxies = json.load(f)
                self.logger.info(f"Loaded {len(proxies)} proxies from {filepath}")
                return proxies
        except Exception as e:
            self.logger.warning(f"Failed to load proxies from {filepath}: {e}")
            return []

    def get_current_proxy_meta(self):
        if not self.proxy_list:
            return {}

        proxy_string = self.proxy_list[self.current_proxy_index]
        try:
            ip, port, user, password = proxy_string.strip().split(":")
            proxy_url = f"http://{ip}:{port}"
            encoded_credentials = base64.b64encode(f"{user}:{password}".encode()).decode()
            return {
                "proxy": proxy_url,
                "headers": {
                    "Proxy-Authorization": f"Basic {encoded_credentials}"
                }
            }
        except ValueError as e:
            self.logger.warning(f"Invalid proxy format: {proxy_string}")
            return {}

    def read_excel_column(self, filepath, column_name):
        try:
            df = pd.read_excel(filepath)
            if column_name not in df.columns:
                raise ValueError(f"Column '{column_name}' not found in {filepath}")
            result = df[column_name].dropna().astype(str).tolist()
            if not result:
                self.logger.warning(f"No valid data in column '{column_name}' of {filepath}")
            return result
        except Exception as e:
            self.logger.error(f"Failed to read {filepath}: {e}")
            return []

    def start_requests(self):
        if not self.whats or not self.wheres:
            self.logger.error("No valid 'what' or 'where' terms to process. Check input files.")
            return
        for what in self.whats:
            for where in self.wheres:
                url = f"https://www.yellowpages.com/search?search_terms={what}&geo_location_terms={where}"
                self.logger.info(f"Starting request for what='{what}', where='{where}'")
                yield from self.make_request(url, {"what": what, "where": where, "page": 1})

    def make_request(self, url, meta, callback=None):
        proxy_meta = self.get_current_proxy_meta()
        headers = self.base_headers.copy()
        headers.update(proxy_meta.get("headers", {}))

        return [scrapy.Request(
            url=url,
            callback=callback or self.parse_listing,
            headers=headers,
            meta={ "proxy": proxy_meta.get("proxy"), **meta },
            dont_filter=True
        )]

    def normalize_text(self, selector):
        if selector is None:
            return ""
        try:
            texts = selector.xpath('.//text()').getall()
        except Exception:
            texts = selector if isinstance(selector, list) else [str(selector)]
        cleaned = []
        for t in texts:
            if t is None:
                continue
            s = unicodedata.normalize("NFKC", t).strip()
            if s:
                cleaned.append(s)
        return " ".join(cleaned)

    def parse_listing(self, response):
        self.total_responses += 1
        if response.status != 200:
            self.errors += 1
            self.logger.warning(f"Non-200 response ({response.status}) for {response.url}")
            if self.proxy_list:
                self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
            yield from self.make_request(response.url, {
                "what": response.meta.get("what"),
                "where": response.meta.get("where"),
                "page": response.meta.get("page", 1)
            })
            return

        what = response.meta["what"]
        where = response.meta["where"]
        page = response.meta.get("page", 1)
        self.logger.info(f"Parsing listings for what='{what}', where='{where}', page={page}")

        listings = response.xpath('//div[@class="result"]')
        self.logger.info(f"Found {len(listings)} listings on page {page}")

        for listing in listings:
            listing_id = listing.xpath('./@id').get()
            if not listing_id or listing_id in self.seen_ids:
                self.duplicate_count += 1
                self.logger.info(f"Skipped duplicate or invalid listing_id={listing_id}")
                continue
            self.seen_ids.add(listing_id)

            company_url = listing.xpath('.//a[@class="business-name"]/@href').get()
            if company_url:
                self.logger.info(f"Requesting company page for listing_id={listing_id}, url={company_url}")
                yield from self.make_request(urljoin(response.url, company_url), {
                    "what": what,
                    "where": where,
                    "page": page,
                    "listing_id": listing_id
                }, callback=self.parse_company)

        next_page_href = response.xpath('//a[@class="next ajax-page"]/@href').get()
        if next_page_href:
            self.logger.info(f"Found next page: {next_page_href}")
            yield from self.make_request(urljoin(response.url, next_page_href), {
                "what": what,
                "where": where,
                "page": page + 1
            })

    def parse_company(self, response):
        self.total_responses += 1
        if response.status != 200:
            self.errors += 1
            self.logger.warning(f"Non-200 response ({response.status}) for {response.url}")
            if self.proxy_list:
                self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
            yield from self.make_request(response.url, {
                "what": response.meta.get("what"),
                "where": response.meta.get("where"),
                "listing_id": response.meta.get("listing_id")
            }, callback=self.parse_company)
            return

        listing_id = response.meta.get("listing_id")
        company_url = response.url
        company = response.xpath('//h1/text()').get() or response.xpath('//h1//span/text()').get()

        business = self.normalize_text(response.xpath('//div[@class="years-in-business"]'))
        yellowpages = self.normalize_text(response.xpath('//div[@class="years-with-yp"]'))
        note = f"{business}, {yellowpages}" if business and yellowpages else business or yellowpages or ""

        raw_categories = response.xpath('//dd/div[@class="categories"]/a/text()').getall()
        categories = [c.strip() for c in raw_categories if c.strip()]
        self.logger.info(f"Categories for listing_id={listing_id}: {categories}")

        # Validate category if forced
        what_value = response.meta.get("what", "").lower()
        if self.category_matching:
            if not any(what_value in c.lower() for c in categories):
                self.excluded_records += 1
                self.logger.info(f"Excluded record for listing_id={listing_id} due to category mismatch")
                return

        primary_phones = [p.strip() for p in response.xpath('//a[@class="phone dockable"]/span[@class="full"]/text()').getall() if p.strip()]
        extra_phones_all = [e.strip() for e in response.xpath('//dd[@class="extra-phones"]//span[2]/text()').getall() if e.strip()]
        specific_extra = response.xpath('(//dd[@class="extra-phones"]//span)[2]/text()').get()
        if specific_extra and specific_extra.strip() and specific_extra.strip() not in extra_phones_all:
            extra_phones_all.append(specific_extra.strip())
        all_phones = primary_phones + extra_phones_all
        phone = primary_phones[0] if primary_phones else ""

        email = ""
        json_ld_scripts = response.xpath('//script[@type="application/ld+json"]/text()').getall()
        for script in json_ld_scripts:
            try:
                data = json.loads(script)
                if isinstance(data, dict) and "email" in data:
                    raw_email = data["email"]
                    candidate = raw_email.replace("mailto:", "").strip() if raw_email.startswith("mailto:") else raw_email.strip()
                    if re.match(r"[^@]+@[^@]+\.[^@]+", candidate):
                        email = candidate
                        break
            except json.JSONDecodeError:
                match = re.search(r'"email"\s*:\s*"(?:mailto:)?([^"]+)"', script)
                if match:
                    candidate = match.group(1).strip()
                    if re.match(r"[^@]+@[^@]+\.[^@]+", candidate):
                        email = candidate
                        break

        website = response.xpath('//a[@class="website-link dockable"]/@href').get()
        website = website.strip() if website else ""

        street_address = response.xpath('//span[@class="address"]/span/text()').get()
        locality = response.xpath('//span[@class="address"]/text()').get()
        city, state, postal_code = None, None, None

        if locality:
            locality = locality.strip()
            m = re.match(r'^(.*?),\s*([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$', locality)
            if m:
                city, state, postal_code = m.groups()
            else:
                parts = [p.strip() for p in locality.split(',') if p.strip()]
                if len(parts) >= 2:
                    city = parts[0]
                    rest_parts = parts[1].split()
                    if len(rest_parts) >= 2:
                        state, postal_code = rest_parts[:2]

        full_address = f"{street_address or ''} {locality or ''}".strip()

        item = {
            "listing_id": listing_id,
            "company": company or "",
            "phone": phone,
            "all_phones": ", ".join(all_phones) if all_phones else "",
            "email": email,
            "website": website,
            "address": street_address or "",
            "city": city or "",
            "state": state or "",
            "postal_code": postal_code or "",
            "full_address": full_address or "",
            "country": "US",
            "what": what_value,
            "where": response.meta.get("where") or "",
            "scraper_source": self.name,
            "source_url": company_url,
            "note": note,
            "category": categories,
            "source": self.source
        }

        self.logger.info(f"Yielding item for listing_id={listing_id}, company={company}")
        yield item

    def closed(self, reason):
        if reason == "finished":
            end_time = datetime.utcnow()
            elapsed = end_time - self.start_time

            what_where_combinations = list(product(self.whats, self.wheres))

            summary_data = {
                "run_id": self.run_id,
                "scraper_name": self.name,
                "source": self.source,
                "where_inputs": sorted(set(self.wheres)),
                "what_inputs": sorted(set(self.whats)),
                "category_matching": self.category_matching,
                "start_time_utc": self.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time_utc": end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "elapsed_seconds": int(elapsed.total_seconds()),
                "elapsed_minutes": int(elapsed.total_seconds() // 60),
                "total_encountered": len(self.seen_ids) + self.duplicate_count,
                "unique_items": len(self.seen_ids),
                "duplicate_items": self.duplicate_count,
                "errors": self.errors,
                "excluded_record_count": self.excluded_records,
                "saved_items": len(self.seen_ids) - self.excluded_records,
                "output_file": self.output_file,
                "summary_file": self.summary_file,
                "notes": reason,
            }
            
            if self.save_to_s3:
                public_output_url = f"https://{self.s3_bucket}.s3.{self.s3_region}.amazonaws.com/{self.s3_key}"
                public_summary_url = f"https://{self.s3_bucket}.s3.{self.s3_region}.amazonaws.com/{self.s3_summary_key}"
                summary_data["output_file"] = f"s3://{self.s3_bucket}/{self.s3_key}"
                summary_data["summary_file"] = f"s3://{self.s3_bucket}/{self.s3_summary_key}"
                summary_data["output_url"] = public_output_url
                summary_data["summary_url"] = public_summary_url
                summary_data["notes"] = reason

                s3_client = boto3.client("s3", region_name=self.s3_region)
                s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=self.s3_summary_key,
                    Body=json.dumps(summary_data, indent=2, ensure_ascii=False).encode("utf-8-sig"),
                    ACL="public-read",
                    ContentType="application/json; charset=utf-8",
                )
            else:
                os.makedirs(os.path.dirname(self.summary_file), exist_ok=True)
                with open(self.summary_file, "w", encoding="utf-8-sig") as f:
                    json.dump(summary_data, f, indent=2, ensure_ascii=False)

            self.logger.info("Summary saved: %s", summary_data.get("summary_file"))