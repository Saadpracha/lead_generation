import scrapy
import os
import pandas as pd
import base64
import json
from urllib.parse import urljoin


class YpCatgSearchSpider(scrapy.Spider):
    name = "yp_catg_search"
    allowed_domains = ["yellowpages.com"]
    base_url = "https://www.yellowpages.com"

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

    def __init__(self, what=None, where=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not what or not where:
            raise ValueError("Please provide both -a what=... and -a where=...")

        self.root_dir = os.getcwd()

        self.proxy_list = self.load_proxies(os.path.join(self.root_dir, "proxies.json"))
        self.proxy_meta = self.get_proxy_meta(self.proxy_list[0]) if self.proxy_list else {}

        self.whats = self.read_excel_column(os.path.join(self.root_dir, what), column_name="what")
        self.wheres = self.read_excel_column(os.path.join(self.root_dir, where), column_name="where")

        self.seen_ids = set()

    def load_proxies(self, filepath):
        with open(filepath, 'r', encoding="utf-8-sig") as f:
            return json.load(f)

    def get_proxy_meta(self, proxy_string):
        ip, port, user, password = proxy_string.strip().split(":")
        proxy_url = f"http://{ip}:{port}"
        credentials = f"{user}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        return {
            'proxy': proxy_url,
            'headers': {
                "Proxy-Authorization": f"Basic {encoded_credentials}"
            }
        }

    def read_excel_column(self, filepath, column_name):
        df = pd.read_excel(filepath)
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' not found in {filepath}")
        return df[column_name].dropna().astype(str).tolist()

    def start_requests(self):
        headers = self.base_headers.copy()
        headers.update(self.proxy_meta.get("headers", {}))

        for what in self.whats:
            for where in self.wheres:
                url = f"https://www.yellowpages.com/search?search_terms={what}&geo_location_terms={where}"
                yield scrapy.Request(
                    url=url,
                    headers=headers,
                    callback=self.parse_search,
                    meta={
                        "proxy": self.proxy_meta.get("proxy"),
                        "what": what,
                        "where": where,
                    },
                    dont_filter=True
                )

    def parse_search(self, response):
        company_urls = response.xpath('//div[@class="info-section info-primary"]/h2/a/@href').extract()

        headers = self.base_headers.copy()
        headers.update(self.proxy_meta.get("headers", {}))

        for relative_url in company_urls:
            full_url = urljoin(self.base_url, relative_url)
            yield scrapy.Request(
                url=full_url,
                headers=headers,
                callback=self.parse_company,
                meta={
                    "proxy": self.proxy_meta.get("proxy"),
                    "search_url": response.url,
                    "company_url": full_url,
                    "what": response.meta["what"],
                    "where": response.meta["where"],
                },
                dont_filter=True
            )

    def parse_company(self, response):
        categories = response.xpath('//dd[@class="categories"]/div/a')
        for cat in categories:
            name = cat.xpath('text()').get()
            href = cat.xpath('@href').get()
            if name and href:
                slug = href.strip('/').split("/")[-1]
                unique_id = f"{slug.lower()}:{name.strip().lower()}"
                if unique_id in self.seen_ids:
                    continue  # Skip duplicates
                self.seen_ids.add(unique_id)

                yield {
                    # "search_url": response.meta["search_url"],
                    # "company_url": response.meta["company_url"],
                    "category_slug": slug.strip(),
                    "category_name": name.strip(),
                    "url": urljoin(self.base_url, href),
                    "what": response.meta["what"],
                    "where": response.meta["where"],
                }
