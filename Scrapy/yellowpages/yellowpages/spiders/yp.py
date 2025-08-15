import scrapy
import json
import base64
import re
import os
from urllib.parse import urljoin, urlparse, parse_qs, unquote
import pandas as pd
from scrapy import Request
from datetime import datetime


class YellowpagesSpider(scrapy.Spider):
    name = "yellowpages"
    allowed_domains = ["yellowpages.ca"]
    BASE = "https://www.yellowpages.ca"
    custom_settings = {
        'DOWNLOAD_FAIL_ON_DATALOSS': False,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        what_file = kwargs.get("what")
        where_file = kwargs.get("where")
        if not what_file or not where_file:
            raise ValueError("You must provide both -a what=<file> and -a where=<file> arguments.")

        self.summary_file = kwargs.get("summary_file", "summary.json")

        self.proxy_list = self.load_proxies(os.path.join(os.getcwd(), "proxies.json"))
        self.what_list = self.load_inputs(what_file)
        self.where_list = self.load_inputs(where_file)

        self.current_proxy_index = 0
        self.seen_listing_ids = set()
        self.total_items_scraped = 0
        self.duplicate_items = 0
        self.total_requests = 0
        self.total_responses = 0
        self.errors = 0
        self.run_id = f"yellowpages-{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
        self.start_time = datetime.utcnow()

    def load_inputs(self, filepath):
        if not os.path.exists(filepath):
            self.logger.warning("Input file not found: %s", filepath)
            return []
        if filepath.endswith(".csv"):
            df = pd.read_csv(filepath)
        elif filepath.endswith(".xlsx"):
            df = pd.read_excel(filepath)
        else:
            self.logger.error("Unsupported file type for input: %s", filepath)
            return []
        col = os.path.splitext(os.path.basename(filepath))[0]
        if col not in df.columns:
            cols = [c for c in df.columns if c.lower() != 'index']
            if not cols:
                return []
            col = cols[0]
        return df[col].dropna().astype(str).tolist()

    def load_proxies(self, proxy_file):
        if not os.path.exists(proxy_file):
            self.logger.warning("Proxy file not found: %s — continuing without proxies", proxy_file)
            return []
        with open(proxy_file, "r", encoding="utf-8-sig") as f:
            raw = json.load(f)
        return raw if isinstance(raw, list) else list(raw.values())

    def start_requests(self):
        for what in self.what_list:
            for where in self.where_list:
                url = f"{self.BASE}/search/si/1/{what}/{where}"
                meta = {"what": what, "where": where}
                yield from self.make_request(url, meta)

    def make_request(self, url, meta):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36",
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
            proxy_data = self.get_proxy_creds(self.current_proxy_index)
            if proxy_data.get("user") and proxy_data.get("pass"):
                creds = f"{proxy_data['user']}:{proxy_data['pass']}"
                headers["Proxy-Authorization"] = "Basic " + base64.b64encode(creds.encode()).decode()
            req_meta["proxy"] = f"http://{proxy_data['ip']}"

        yield Request(url, headers=headers, meta=req_meta, callback=self.parse, errback=self.handle_error, dont_filter=True)

    def get_proxy_creds(self, index):
        if not self.proxy_list:
            return {"ip": "", "user": "", "pass": ""}
        entry = self.proxy_list[index % len(self.proxy_list)]
        parts = entry.split(":")
        if len(parts) == 4:
            ip, port, user, password = parts
            return {"ip": f"{ip}:{port}", "user": user, "pass": password}
        if len(parts) == 2:
            return {"ip": entry, "user": "", "pass": ""}
        return {"ip": entry, "user": "", "pass": ""}

    def handle_error(self, failure):
        req = getattr(failure, "request", None)
        if req is None:
            self.logger.error("Failure missing request: %s", failure)
            return
        if self.proxy_list:
            prev = self.current_proxy_index
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
            self.logger.warning("Request failed: %s — rotating proxy %d -> %d and retrying", req.url, prev, self.current_proxy_index)
            meta = {"what": req.meta.get("what"), "where": req.meta.get("where")}
            yield from self.make_request(req.url, meta)
        else:
            self.logger.error("Request failed and no proxies available to rotate for: %s", req.url)

    def parse(self, response):
        self.total_responses += 1
        if response.status != 200:
            if self.proxy_list:
                prev = self.current_proxy_index
                self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
                self.logger.warning("Non-200 (%s) on %s — rotating proxy %d -> %d and retrying", response.status, response.url, prev, self.current_proxy_index)
                yield from self.make_request(response.url, {"what": response.meta.get("what"), "where": response.meta.get("where")})
            else:
                self.logger.error("Non-200 and no proxies to rotate: %s (%s)", response.url, response.status)
            return

        listings = response.xpath('//div[contains(@class, "listing__content")]')
        for listing in listings:
            name = listing.xpath('.//a[contains(@class,"jsListingName")]/text()').get(default="N/A").strip()
            link = listing.xpath('.//a[contains(@class,"jsListingName")]/@href').get()
            full_link = urljoin(self.BASE, link) if link else None

            listing_id_match = re.search(r"/(\d+)\.html", link or "")
            listing_id = listing_id_match.group(1) if listing_id_match else None

            phones = [p.strip() for p in listing.xpath('.//ul[contains(@class,"mlr__submenu")]//h4/text()').getall() if p.strip()]
            first_phone = phones[0] if phones else ""

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
            self.total_items_scraped += 1

            sponsored = bool(listing.xpath('.//span[@class="listing__placement"]'))
            categories = [c.strip() for c in listing.xpath('.//div[contains(@class,"listing__headings")]//a/text()').getall() if c.strip()] or ["N/A"]

            street = listing.xpath('.//span[@itemprop="streetAddress"]/text()').get(default="").strip()
            locality = listing.xpath('.//span[@itemprop="addressLocality"]/text()').get(default="").strip()
            region = listing.xpath('.//span[@itemprop="addressRegion"]/text()').get(default="").strip()
            postal = listing.xpath('.//span[@itemprop="postalCode"]/text()').get(default="").strip()
            full_address = ", ".join(filter(None, [street, locality, region, postal])) or "N/A"

            website_suffix = listing.xpath('.//li[contains(@class,"mlr__item--website")]/a/@href').get()
            website = "N/A"
            if website_suffix:
                if "/gourl/" in website_suffix or "redirect=" in website_suffix:
                    redirect_url = parse_qs(urlparse(website_suffix).query).get("redirect", [None])[0]
                    website = unquote(redirect_url) if redirect_url else website_suffix
                else:
                    website = urljoin(self.BASE, website_suffix)

            yield {
                "Name": name,
                "Listing_id": listing_id or "N/A",
                "Profile_url": full_link,
                "Street_address": street or "N/A",
                "City": locality or "N/A",
                "Province": region or "N/A",
                "Postal_code": postal or "N/A",
                "Full_address": full_address,
                "Phones": phones or ["N/A"],
                "Website": website,
                "Categories": categories,
                "Sponsored": sponsored,
                "What": response.meta.get("what"),
                "Where": response.meta.get("where"),
            }

        next_page = response.xpath('//a[contains(text(), "Next")]/@href').get()
        if next_page:
            yield from self.make_request(urljoin(self.BASE, next_page), {"what": response.meta.get("what"), "where": response.meta.get("where")})

    from datetime import timedelta

    def closed(self, reason):
        end_time = datetime.utcnow()
        elapsed_time = end_time - self.start_time

        # Format the start and end times to a more user-friendly format (YYYY-MM-DD HH:MM:SS)
        formatted_start_time = self.start_time.strftime("%Y-%m-%d %H:%M:%S")
        formatted_end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")

        # Elapsed time in seconds and minutes
        elapsed_seconds = int(elapsed_time.total_seconds())
        elapsed_minutes = elapsed_seconds // 60  # Integer division to avoid floating-point values

        # Create summary data
        summary_data = {
            "run_id": self.run_id,
            "start_time_utc": formatted_start_time,
            "end_time_utc": formatted_end_time,
            "elapsed_seconds": elapsed_seconds,
            "elapsed_minutes": elapsed_minutes,
            "total_responses": self.total_responses,
            "total_items_scraped": self.total_items_scraped + self.duplicate_items,
            "duplicate_items": self.duplicate_items,
            "saved_items": self.total_items_scraped,
            "errors": self.errors,
            "notes": reason,
        }

        # Save summary to file
        with open(self.summary_file, "w", encoding="utf-8") as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
