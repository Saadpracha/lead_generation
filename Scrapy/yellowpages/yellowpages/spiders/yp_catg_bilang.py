import scrapy
import os
import json
import base64
import re


class YpCategoriesSpider(scrapy.Spider):
    name = "yp_categories_bilingual"

    base_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    domains = {
        'en': 'www.yellowpages.ca',
        'fr': 'www.pagesjaunes.ca'
    }

    def __init__(self, lang='en', *args, **kwargs):
        super().__init__(*args, **kwargs)

        if lang not in self.domains:
            raise ValueError("Invalid language. Use 'en' or 'fr'.")

        self.lang = lang
        self.host = self.domains[lang]
        self.start_urls = [f"https://{self.host}/business/"]
        self.allowed_domains = [self.host]
        self.base_headers["Host"] = self.host

        self.proxy_list = self.load_proxies(os.path.join(os.getcwd(), "proxies.json"))
        self.proxy_meta = self.get_proxy_meta(self.proxy_list[0]) if self.proxy_list else {}
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

    def start_requests(self):
        headers = self.base_headers.copy()
        headers.update(self.proxy_meta.get("headers", {}))

        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                headers=headers,
                callback=self.parse_level_1,
                meta={
                    "proxy": self.proxy_meta.get("proxy"),
                    "lang": self.lang,
                    "breadcrumb": []
                }
            )

    def parse_level_1(self, response):
        names = response.xpath('//div[@class="categories-wrap catWrap brow"]/h3//a/text()').getall()
        urls = response.xpath('//div[@class="categories-wrap catWrap brow"]/h3//a/@href').getall()

        for name, rel_url in zip(names, urls):
            url = response.urljoin(rel_url)
            listing_id = self.extract_listing_id(url)
            headers = self.base_headers.copy()
            headers.update(self.proxy_meta.get("headers", {}))

            meta = {
                "proxy": self.proxy_meta.get("proxy"),
                "lang": self.lang,
                "level_1_name_" + self.lang: name.strip(),
                "level_1_url": url,
                "level_1_id": listing_id,
                "breadcrumb": [name.strip()],
                "slug_path": rel_url.strip("/"),
            }

            yield scrapy.Request(
                url=url,
                headers=headers,
                callback=self.parse_level_2,
                meta=meta
            )

    def parse_level_2(self, response):
        names = response.xpath('//div[@class="categories-wrap catWrap brow"]/h3//a/text()').getall()
        urls = response.xpath('//div[@class="categories-wrap catWrap brow"]/h3//a/@href').getall()

        if not urls:
            yield from self.parse_level_3(response)
            return

        for name, rel_url in zip(names, urls):
            url = response.urljoin(rel_url)
            listing_id = self.extract_listing_id(url)
            headers = self.base_headers.copy()
            headers.update(self.proxy_meta.get("headers", {}))

            meta = response.meta.copy()
            meta.update({
                "level_2_name_" + self.lang: name.strip(),
                "level_2_url": url,
                "level_2_id": listing_id,
                "breadcrumb": response.meta["breadcrumb"] + [name.strip()],
                "slug_path": f"{response.meta['slug_path']}/{rel_url.strip('/')}"
            })

            yield scrapy.Request(
                url=url,
                headers=headers,
                callback=self.parse_level_3,
                meta=meta
            )

    def parse_level_3(self, response):
        names = response.xpath('//ul[@class="categories-list"]//a/text()').getall()
        urls = response.xpath('//ul[@class="categories-list"]//a/@href').getall()

        if not urls:
            return

        for name, rel_url in zip(names, urls):
            url = response.urljoin(rel_url)
            listing_id = self.extract_listing_id(url)

            if listing_id and listing_id in self.seen_ids:
                continue
            if listing_id:
                self.seen_ids.add(listing_id)

            data = {
                "level_1_id": response.meta.get("level_1_id"),
                "level_1_name_en": response.meta.get("level_1_name_en"),
                "level_1_name_fr": response.meta.get("level_1_name_fr"),
                "level_1_url": response.meta.get("level_1_url"),

                "level_2_id": response.meta.get("level_2_id"),
                "level_2_name_en": response.meta.get("level_2_name_en"),
                "level_2_name_fr": response.meta.get("level_2_name_fr"),
                "level_2_url": response.meta.get("level_2_url"),

                "level_3_id": listing_id,
                "level_3_name_en": name.strip() if self.lang == 'en' else None,
                "level_3_name_fr": name.strip() if self.lang == 'fr' else None,
                "level_3_url": url,

                "leaf_name_en": name.strip() if self.lang == 'en' else None,
                "leaf_name_fr": name.strip() if self.lang == 'fr' else None,
                "leaf_url": url,

                "slug_path": f"{response.meta.get('slug_path')}/{rel_url.strip('/')}",
                "source_path_en": " > ".join(response.meta["breadcrumb"]) if self.lang == "en" else None,
                "source_path_fr": " > ".join(response.meta["breadcrumb"]) if self.lang == "fr" else None,
            }

            yield data

    def extract_listing_id(self, url):
        match1 = re.search(r"/business/(\d+)\.html", url)
        if match1:
            return match1.group(1)

        match2 = re.search(r"/products/([\w/]+)/?", url)
        if match2:
            return match2.group(1)

        return None
