import scrapy
import os
import json
import base64
import re


class YpCategoriesSpider(scrapy.Spider):
    name = "yp_categories"
    allowed_domains = ["yellowpages.ca"]
    start_urls = ["https://www.yellowpages.ca/business/"]

    base_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Host": "www.yellowpages.ca",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.proxy_list = self.load_proxies(os.path.join(os.getcwd(), "proxies.json"))
        self.proxy_meta = self.get_proxy_meta(self.proxy_list[0]) if self.proxy_list else {}
        self.seen_ids = set()

    def load_proxies(self, filepath):
        with open(filepath, 'r',encoding="utf-8-sig") as f:
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

    def start_requests(self):
        headers = self.base_headers.copy()
        headers.update(self.proxy_meta.get("headers", {}))

        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                headers=headers,
                callback=self.parse_level_1,
                meta={"proxy": self.proxy_meta.get("proxy")}
            )

    def parse_level_1(self, response):
        names = response.xpath('//div[@class="categories-wrap catWrap brow"]/h3//a/text()').getall()
        urls = response.xpath('//div[@class="categories-wrap catWrap brow"]/h3//a/@href').getall()

        for name, rel_url in zip(names, urls):
            url = response.urljoin(rel_url)
            headers = self.base_headers.copy()
            headers.update(self.proxy_meta.get("headers", {}))

            yield scrapy.Request(
                url=url,
                headers=headers,
                callback=self.parse_level_2,
                meta={
                    "proxy": self.proxy_meta.get("proxy"),
                    "level_1_name": name.strip(),
                    "level_1_url": url
                }
            )

    def parse_level_2(self, response):
        names = response.xpath('//div[@class="categories-wrap catWrap brow"]/h3/a/text()').getall()
        urls = response.xpath('//div[@class="categories-wrap catWrap brow"]/h3/a/@href').getall()

        headers = self.base_headers.copy()
        headers.update(self.proxy_meta.get("headers", {}))

        if not urls:
            yield from self.parse_level_3(response)

        for name, rel_url in zip(names, urls):
            url = response.urljoin(rel_url)
            yield scrapy.Request(
                url=url,
                headers=headers,
                callback=self.parse_level_3,
                meta={
                    "proxy": self.proxy_meta.get("proxy"),
                    "level_1_name": response.meta.get("level_1_name"),
                    "level_1_url": response.meta.get("level_1_url"),
                    "level_2_name": name.strip(),
                    "level_2_url": url
                }
            )

    def parse_level_3(self, response):
        names = response.xpath('//ul[@class="categories-list"]//a/text()').getall()
        urls = response.xpath('//ul[@class="categories-list"]//a/@href').getall()

        if not urls:
            return

        for name, rel_url in zip(names, urls):
            url = response.urljoin(rel_url)
            listing_id = self.extract_listing_id(url)

            if listing_id and listing_id not in self.seen_ids:
                self.seen_ids.add(listing_id)
                yield {
                    "level_1_name": response.meta.get("level_1_name"),
                    "level_1_url": response.meta.get("level_1_url"),
                    "level_2_name": response.meta.get("level_2_name"),
                    "level_2_url": response.meta.get("level_2_url"),
                    "level_3_name": name.strip(),
                    "level_3_url": url,
                    "listing_id": listing_id
                }

    def extract_listing_id(self, url):
        # Match /business/00713000.html
        match1 = re.search(r"/business/(\d+)\.html", url)
        if match1:
            return match1.group(1)

        # Match /products/01125710/E00001028/
        match2 = re.search(r"/products/([\w/]+)/?", url)
        if match2:
            return match2.group(1)

        return None
