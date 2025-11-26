import scrapy
import os
import json
import base64
from urllib.parse import urljoin


class YpLocationSpider(scrapy.Spider):
    name = "yp_location"
    allowed_domains = ["yellowpages.ca"]
    start_urls = ["https://www.yellowpages.ca/locations/"]

    base_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Host": "www.yellowpages.ca",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        proxies_path = os.path.join(os.getcwd(), "proxies.json")
        # proxies.json expected as a list of strings like "ip:port:user:pass" or "ip:port"
        self.proxy_list = []
        if os.path.exists(proxies_path):
            with open(proxies_path, "r", encoding="utf-8-sig") as f:
                self.proxy_list = json.load(f) or []
        # use first proxy by default; can rotate later
        self.proxy_meta = self._get_proxy_meta(self.proxy_list[0]) if self.proxy_list else {}
        # avoid revisiting same city in same province
        self.seen_cities = set()

    def _get_proxy_meta(self, proxy_string):
        """
        Accept "ip:port:user:pass" or "ip:port".
        Returns {'proxy': 'http://ip:port', 'auth_header': {'Proxy-Authorization': 'Basic ...'}} (auth_header optional)
        """
        parts = proxy_string.strip().split(":")
        if len(parts) < 2:
            return {}
        ip = parts[0]
        port = parts[1]
        proxy_url = f"http://{ip}:{port}"
        meta = {"proxy": proxy_url}
        if len(parts) >= 4:
            user = parts[2]
            password = parts[3]
            creds = f"{user}:{password}"
            encoded = base64.b64encode(creds.encode()).decode()
            meta["auth_header"] = {"Proxy-Authorization": f"Basic {encoded}"}
        return meta

    def _make_request(self, url, callback, meta_extra=None, dont_filter=False, cb_kwargs=None):
        """
        Build a Request that always includes proxy meta and Proxy-Authorization header (if available).
        """
        headers = self.base_headers.copy()
        if self.proxy_meta.get("auth_header"):
            headers.update(self.proxy_meta["auth_header"])
        meta = meta_extra.copy() if meta_extra else {}
        if self.proxy_meta.get("proxy"):
            meta["proxy"] = self.proxy_meta["proxy"]
        return scrapy.Request(url=response_url_join(url) if isinstance(url, str) else url,
                              headers=headers,
                              callback=callback,
                              meta=meta,
                              dont_filter=dont_filter,
                              cb_kwargs=cb_kwargs or {})

    def start_requests(self):
        for url in self.start_urls:
            headers = self.base_headers.copy()
            if self.proxy_meta.get("auth_header"):
                headers.update(self.proxy_meta["auth_header"])
            meta = {}
            if self.proxy_meta.get("proxy"):
                meta["proxy"] = self.proxy_meta["proxy"]
            yield scrapy.Request(url=url, headers=headers, callback=self.parse, meta=meta)

    def parse(self, response):
        # provinces links
        provinces = response.xpath('//h3[contains(@class,"categories-title") or contains(@class,"catTitle")]/a')
        for province in provinces:
            province_name = province.xpath('normalize-space(text())').get()
            province_url = response.urljoin(province.xpath('@href').get() or "")
            meta = {'province_name': province_name, 'province_url': province_url}
            # create request with same proxy/auth header
            headers = self.base_headers.copy()
            if self.proxy_meta.get("auth_header"):
                headers.update(self.proxy_meta["auth_header"])
            req_meta = meta.copy()
            if self.proxy_meta.get("proxy"):
                req_meta["proxy"] = self.proxy_meta["proxy"]
            yield scrapy.Request(url=province_url, headers=headers, callback=self.parse_province, meta=req_meta)

    def parse_province(self, response):
        province_name = response.meta.get('province_name')
        province_url = response.meta.get('province_url')

        # Initialize meta here to avoid UnboundLocalError
        meta = {'province_name': province_name, 'province_url': province_url}

        # city list
        cities = response.xpath('//div[@class="categories-wrap catWrap brow"]//li/a')
        for city in cities:
            city_name = city.xpath('normalize-space(text())').get()
            city_url = response.urljoin(city.xpath('@href').get() or "")
            key = (province_name or "", city_name or "")
            if city_name and key not in self.seen_cities:
                self.seen_cities.add(key)
                meta = {
                    'province_name': province_name,
                    'province_url': province_url,
                    'city_name': city_name,
                    'city_url': city_url
                }

        # pagination / more links
        more_links = response.xpath('//span[contains(text(), "Cities in")]/parent::p/a/@href').getall()
        for link in more_links:
            absolute = response.urljoin(link)
            headers = self.base_headers.copy()
            if self.proxy_meta.get("auth_header"):
                headers.update(self.proxy_meta["auth_header"])
            if self.proxy_meta.get("proxy"):
                meta["proxy"] = self.proxy_meta["proxy"]
            yield scrapy.Request(url=absolute, headers=headers, callback=self.parse_city, meta=meta)


    def parse_city(self, response):
        province_name = response.meta.get('province_name')
        province_url = response.meta.get('province_url')
        city_name = response.meta.get('city_name', 'Unknown')
        city_url = response.meta.get('city_url', response.url)

        # if page contains nested city list, emit each entry
        cities = response.xpath('//div[@class="categories-wrap catWrap brow"]//li/a')
        if cities:
            for city in cities:
                c_name = city.xpath('normalize-space(text())').get()
                c_url = response.urljoin(city.xpath('@href').get() or "")
                key = (province_name or "", c_name or "")
                if c_name and key not in self.seen_cities:
                    self.seen_cities.add(key)
                    yield {
                        'country': 'Canada',
                        'country_url': 'https://www.yellowpages.ca/locations/',
                        'province': province_name,
                        'province_url': province_url,
                        'city': c_name,
                        'city_url': c_url
                    }
        else:
            yield {
                'country': 'Canada',
                'country_url': 'https://www.yellowpages.ca/locations/',
                'province': province_name,
                'province_url': province_url,
                'city': city_name,
                'city_url': city_url
            }


# small utility to ensure urljoin works uniformly when passing raw hrefs into _make_request
def response_url_join(href):
    # If href already absolute, return it; else return it as-is (Scrapy will resolve relative in Request if needed)
    return href
