import scrapy
import os
import json
import base64


class YPCanadaProductSpider(scrapy.Spider):
    name = "yp_canada_product"
    allowed_domains = ["yellowpages.ca"]
    start_urls = ["https://www.yellowpages.ca/products/"]

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
                meta={"proxy": self.proxy_meta.get("proxy")}
            )

    def parse_level_1(self, response):
        headers = self.base_headers.copy()
        headers.update(self.proxy_meta.get("headers", {}))

        names = response.xpath('//h3[@class="categories-title catTitle"]/a/text()').getall()
        urls = response.xpath('//h3[@class="categories-title catTitle"]/a/@href').getall()

        for name, url in zip(names, urls):
            full_url = response.urljoin(url)
            if "search/si" in full_url:
                continue

            if "/products/" in full_url:
                if full_url in self.seen_ids:
                    continue
                self.seen_ids.add(full_url)

                yield {
                    "level_1_name_en": name.strip(),
                    "level_1_url": full_url,
                    "level_2_name_en": "",
                    "level_2_url": "",
                    "leaf_name_en": name.strip(),
                    "leaf_url": full_url,
                    "slug_path": full_url.replace("https://www.yellowpages.ca", ""),
                    "source_path_en": full_url
                }
            else:
                yield scrapy.Request(
                    url=full_url,
                    headers=headers,
                    callback=self.parse_level_2,
                    meta={
                        "proxy": self.proxy_meta.get("proxy"),
                        "level_1_name_en": name.strip(),
                        "level_1_url": full_url
                    }
                )

    def parse_level_2(self, response):
        headers = self.base_headers.copy()
        headers.update(self.proxy_meta.get("headers", {}))

        names = response.xpath('//h3[@class="categories-title catTitle"]/a/text()').getall()
        urls = response.xpath('//h3[@class="categories-title catTitle"]/a/@href').getall()

        for name, url in zip(names, urls):
            full_url = response.urljoin(url)
            if "search/si" in full_url:
                continue

            if "/products/" in full_url:
                if full_url in self.seen_ids:
                    continue
                self.seen_ids.add(full_url)

                yield {
                    "level_1_name_en": response.meta["level_1_name_en"],
                    "level_1_url": response.meta["level_1_url"],
                    "level_2_name_en": name.strip(),
                    "level_2_url": full_url,
                    "leaf_name_en": name.strip(),
                    "leaf_url": full_url,
                    "slug_path": full_url.replace("https://www.yellowpages.ca", ""),
                    "source_path_en": full_url
                }
            else:
                yield scrapy.Request(
                    url=full_url,
                    headers=headers,
                    callback=self.parse_level_3,
                    meta={
                        "proxy": self.proxy_meta.get("proxy"),
                        "level_1_name_en": response.meta["level_1_name_en"],
                        "level_1_url": response.meta["level_1_url"],
                        "level_2_name_en": name.strip(),
                        "level_2_url": full_url
                    }
                )

    def parse_level_3(self, response):
        headers = self.base_headers.copy()
        headers.update(self.proxy_meta.get("headers", {}))

        product_names = response.xpath('//div[@class="categories-wrap catWrap res"]//li/a/text()').getall()
        product_urls = response.xpath('//div[@class="categories-wrap catWrap res"]//li/a/@href').getall()

        for name, url in zip(product_names, product_urls):
            full_url = response.urljoin(url)
            if "search/si" in full_url or "/products/" not in full_url:
                continue

            if full_url in self.seen_ids:
                continue
            self.seen_ids.add(full_url)

            yield {
                "level_1_name_en": response.meta["level_1_name_en"],
                "level_1_url": response.meta["level_1_url"],
                "level_2_name_en": response.meta.get("level_2_name_en", ""),
                "level_2_url": response.meta.get("level_2_url", ""),
                "leaf_name_en": name.strip(),
                "leaf_url": full_url,
                "slug_path": full_url.replace("https://www.yellowpages.ca", ""),
                "source_path_en": full_url
            }
