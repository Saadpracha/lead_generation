# Scrapers/spiders/yp_canada.py
import os
import re
import json
import base64
from itertools import product
from urllib.parse import urljoin, urlparse, parse_qs, unquote
from datetime import datetime
import boto3
import pandas as pd
import scrapy
from scrapy import Request
from Scrapers import settings



class YellowpagesCanadaSpider(scrapy.Spider):
    name = "yellowpages_canada"
    allowed_domains = ["yellowpages.ca"]
    BASE = "https://www.yellowpages.ca"

    custom_settings = {
        "DOWNLOAD_FAIL_ON_DATALOSS": False,
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

        # ✅ Now log after everything is initialized
        if spider.save_to_s3:
            spider.logger.info(f"Output will be saved to S3: s3://{spider.s3_bucket}/{spider.s3_key}")
        else:
            spider.logger.info(f"Output file will be saved locally to: {spider.output_file}")

        return spider


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Required arguments (passed with -a)
        what_file = kwargs.get("what")
        where_file = kwargs.get("where")
        self.dir_name = kwargs.get("dir_name")
        output_file = kwargs.get("output_file", "output.csv")

        if not what_file or not where_file:
            raise ValueError("You must provide both -a what=<file> and -a where=<file> arguments.")
        if not self.dir_name:
            raise ValueError("You must provide -a dir_name=<subdir_name> argument.")
        if not output_file.endswith('.csv'):
            raise ValueError("Output file must have a .csv extension")

        # Optional arguments
        self.source = kwargs.get("source", "")
        self.category_matching = kwargs.get("category_matching", "no").lower() == "yes"

        # Output directory base mapping
        output_base_dirs = {
            "yellowpages_canada": os.path.join("imp_data", "YP_Canada", "output")
        }
        base_dir = output_base_dirs.get(self.name)
        if not base_dir:
            raise ValueError(f"No output directory configured for spider '{self.name}'")

        # Construct output paths
        self.output_dir = os.path.join(base_dir, self.dir_name)
        os.makedirs(self.output_dir, exist_ok=True)
        self.output_file = os.path.join(self.output_dir, output_file)
        self.summary_file = os.path.join(self.output_dir, kwargs.get("summary", "summary.json"))
        # prepare S3 keys like yp_us (use forward slashes)
        self.s3_key = self.output_file.replace(os.sep, "/")
        self.s3_summary_key = self.summary_file.replace(os.sep, "/")

        # load inputs (what/where) from provided files
        self.what_list = self.load_inputs(what_file)
        self.where_list = self.load_inputs(where_file)
        if not self.what_list:
            raise ValueError(f"No entries loaded from what file: {what_file}")
        if not self.where_list:
            raise ValueError(f"No entries loaded from where file: {where_file}")

        # Set root dir to current working directory (where you run Scrapy)
        self.root_dir = os.getcwd()

        # Load proxies.json from root (where scrapy.cfg and proxies.json exist)
        proxy_file = os.path.join(self.root_dir, "proxies.json")
        if os.path.exists(proxy_file):
            self.proxy_list = self.load_proxies(proxy_file)
            self.logger.info(f"Loaded {len(self.proxy_list)} proxies from {proxy_file}")
        else:
            self.proxy_list = []
            self.logger.warning("No proxy file found at: %s — continuing without proxies", proxy_file)

        # Initialize runtime state
        self.current_proxy_index = 0
        self.seen_listing_ids = set()
        self.total_items_scraped = 0
        self.duplicate_items = 0
        self.excluded_items = 0
        self.total_requests = 0
        self.total_responses = 0
        self.errors = 0
        self.run_id = f"{self.name}-{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
        self.start_time = datetime.utcnow()

    # ---------- helpers ----------
    def load_inputs(self, filepath):
        """Load a CSV or Excel where the column name is inferred from filename or fallback to the first non-index column."""
        if not os.path.exists(filepath):
            self.logger.warning("Input file not found: %s", filepath)
            return []

        if filepath.endswith(".csv"):
            df = pd.read_csv(filepath)
        elif filepath.endswith(".xlsx") or filepath.endswith(".xls"):
            df = pd.read_excel(filepath)
        else:
            self.logger.error("Unsupported input file type: %s", filepath)
            return []

        col = os.path.splitext(os.path.basename(filepath))[0]
        if col not in df.columns:
            # fallback to first non-index column
            cols = [c for c in df.columns if c.lower() != "index"]
            if not cols:
                return []
            col = cols[0]

        return df[col].dropna().astype(str).tolist()

    def load_proxies(self, proxy_file):
        """Load proxies from a JSON file (list or dict). Expect entries like ip:port:user:pass or ip:port."""
        if not os.path.exists(proxy_file):
            self.logger.warning("Proxy file not found: %s — continuing without proxies", proxy_file)
            return []

        with open(proxy_file, "r", encoding="utf-8-sig") as f:
            raw = json.load(f)
            return raw if isinstance(raw, list) else list(raw.values())

    def get_proxy_creds(self, index):
        if not self.proxy_list:
            return {"ip": "", "user": "", "pass": ""}

        entry = self.proxy_list[index % len(self.proxy_list)]
        parts = entry.split(":")
        if len(parts) == 4:
            ip, port, user, password = parts
            return {"ip": f"{ip}:{port}", "user": user, "pass": password}
        return {"ip": entry, "user": "", "pass": ""}

    # ---------- request flow ----------
    def start_requests(self):
        for what in self.what_list:
            for where in self.where_list:
                url = f"{self.BASE}/search/si/1/{what}/{where}"
                meta = {"what": what, "where": where}
                yield from self.make_request(url, meta)

    def make_request(self, url, meta):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Host": "www.yellowpages.ca",
        }

        req_meta = {
            "what": meta.get("what"),
            "where": meta.get("where"),
            "dont_retry": True,
            "handle_httpstatus_list": [400, 403, 404, 429, 500, 502, 503, 504],
        }

        if self.proxy_list:
            proxy = self.get_proxy_creds(self.current_proxy_index)
            if proxy["user"] and proxy["pass"]:
                creds = f"{proxy['user']}:{proxy['pass']}"
                headers["Proxy-Authorization"] = "Basic " + base64.b64encode(creds.encode()).decode()
            req_meta["proxy"] = f"http://{proxy['ip']}"

        self.total_requests += 1
        yield Request(
            url,
            headers=headers,
            meta=req_meta,
            callback=self.parse,
            errback=self.handle_error,
            dont_filter=True,
        )

    def handle_error(self, failure):
        req = getattr(failure, "request", None)
        if not req:
            self.logger.error("Failure missing request: %s", failure)
            self.errors += 1
            return

        if self.proxy_list:
            prev = self.current_proxy_index
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
            self.logger.warning(
                "Request failed: %s — rotating proxy %d -> %d and retrying",
                req.url, prev, self.current_proxy_index
            )
            yield from self.make_request(req.url, req.meta)
        else:
            self.logger.error("Request failed and no proxies available: %s", req.url)
            self.errors += 1

    # ---------- parsing ----------
    def parse(self, response):
        self.total_responses += 1

        if response.status != 200:
            if self.proxy_list:
                prev = self.current_proxy_index
                self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
                self.logger.warning("Non-200 (%s) on %s — rotating proxy %d -> %d", response.status, response.url, prev, self.current_proxy_index)
                yield from self.make_request(response.url, response.meta)
            else:
                self.logger.error("Non-200 and no proxies to rotate: %s (%s)", response.url, response.status)
            return

        listings = response.xpath('//div[contains(@class, "listing__content")]')

        for listing in listings:
            name = listing.xpath('.//a[contains(@class,"jsListingName")]/text()').get(default="").strip()
            link = listing.xpath('.//a[contains(@class,"jsListingName")]/@href').get()
            full_link = urljoin(self.BASE, link) if link else ""

            listing_id = re.search(r"/(\d+)\.html", link or "")
            listing_id = listing_id.group(1) if listing_id else None

            phone_texts = listing.xpath('.//ul[contains(@class,"mlr__submenu")]//text()').getall()
            if not phone_texts:
                phone_texts = listing.xpath('.//text()').getall()

            phone_pattern = re.compile(r'(\+?\d[\d\-\s().]{6,}\d)')
            phones = []
            for t in phone_texts:
                t = t.strip()
                m = phone_pattern.search(t)
                if m:
                    candidate = re.sub(r'\s+', ' ', m.group(1)).strip()
                    if candidate not in phones:
                        phones.append(candidate)

            first_phone = phones[0] if phones else ""
            all_phones_csv = ",".join(phones)

            # Deduplication key
            if listing_id:
                dedupe_key = f"id:{listing_id}"
            elif full_link:
                dedupe_key = f"url:{full_link}"
            else:
                dedupe_key = f"name_phone:{name.lower()}_{first_phone}"

            if dedupe_key in self.seen_listing_ids:
                self.duplicate_items += 1
                continue
            self.seen_listing_ids.add(dedupe_key)

            # Sponsored flag
            sponsored = bool(listing.xpath('.//span[@class="listing__placement"]'))

            # Categories
            cats = listing.xpath('.//div[contains(@class,"listing__headings")]//a/text()').getall()
            if not cats:
                raw = listing.xpath('.//div[contains(@class,"listing__headings")]//text()').get()
                cats = re.split(r',\s*', raw) if raw else []

            cleaned_cats = [re.sub(r'\s*More(?:\.\.\.|…)?$', '', c).strip()
                            for c in cats if not re.match(r'(?i)^more(?:\.\.\.|…)?$', c.strip())]
            categories = list(filter(None, cleaned_cats))

            if self.category_matching:
                what = response.meta.get("what", "").lower()
                if not any(what in c.lower() for c in categories):
                    self.excluded_items += 1
                    continue

            # Address
            street = listing.xpath('.//span[@itemprop="streetAddress"]/text()').get(default="").strip()
            locality = listing.xpath('.//span[@itemprop="addressLocality"]/text()').get(default="").strip()
            region = listing.xpath('.//span[@itemprop="addressRegion"]/text()').get(default="").strip()
            postal = listing.xpath('.//span[@itemprop="postalCode"]/text()').get(default="").strip()
            full_address = ", ".join(filter(None, [street, locality, region, postal]))

            # Website
            website_suffix = listing.xpath('.//li[contains(@class,"mlr__item--website")]/a/@href').get()
            website = ""
            if website_suffix:
                try:
                    if "/gourl/" in website_suffix or "redirect=" in website_suffix:
                        redirect_url = parse_qs(urlparse(website_suffix).query).get("redirect", [None])[0]
                        website = unquote(redirect_url) if redirect_url else website_suffix
                    else:
                        website = urljoin(self.BASE, website_suffix)
                except Exception:
                    website = website_suffix

            self.total_items_scraped += 1

            yield {
                "listing_id": listing_id,
                "company": name,
                "phone": first_phone,
                "all_phones": all_phones_csv,
                "email": "",
                "website": website,
                "address": street,
                "city": locality,
                "state": region,
                "postal_code": postal,
                "full_address": full_address,
                "country": "CA",
                "what": response.meta.get("what"),
                "where": response.meta.get("where"),
                "scraper_source": self.name,
                "source_url": full_link,
                "note": "Sponsored" if sponsored else "",
                "category": categories,
                "source": self.source,
            }

        # pagination
        next_page = response.xpath('//a[contains(text(), "Next")]/@href').get()
        if next_page:
            yield from self.make_request(urljoin(self.BASE, next_page), response.meta)

    # ---------- cleanup (summary) ----------
    def closed(self, reason):
        if reason == "finished":
            end_time = datetime.utcnow()
            elapsed = end_time - self.start_time

            # Make summary similar to yp_us
            what_where_combinations = list(product(self.what_list, self.where_list))

            summary_data = {
                "run_id": self.run_id,
                "scraper_name": self.name,
                "source": self.source,
                "where_inputs": sorted(set(self.where_list)),
                "what_inputs": sorted(set(self.what_list)),
                "category_matching": self.category_matching,
                "start_time_utc": self.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time_utc": end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "elapsed_seconds": int(elapsed.total_seconds()),
                "elapsed_minutes": int(elapsed.total_seconds() // 60),
                "total_encountered": len(self.seen_listing_ids) + int(self.duplicate_items // 2 - len(self.seen_listing_ids)),
                "unique_items": len(self.seen_listing_ids),
                "duplicate_items": int(self.duplicate_items // 2 - len(self.seen_listing_ids)),
                "errors": self.errors,
                "excluded_record_count": self.excluded_items,
                "saved_items": max(0, len(self.seen_listing_ids) - self.excluded_items),
                "output_file": f"s3://{self.s3_bucket}/{self.s3_key}" if self.save_to_s3 else self.output_file,
                "summary_file": f"s3://{self.s3_bucket}/{self.s3_summary_key}" if self.save_to_s3 else self.summary_file,
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