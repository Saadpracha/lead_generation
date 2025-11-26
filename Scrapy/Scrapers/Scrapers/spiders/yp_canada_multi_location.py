import scrapy
import os
import json
import base64
import re
import logging  # Add logging for debugging

class YPCanadaMultiLocationSpider(scrapy.Spider):
    name = "yp_canada_multi_location"
    allowed_domains = ["yellowpages.ca"]
    start_urls = ["https://www.yellowpages.ca/national"]

    base_url = "https://www.yellowpages.ca"

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
                callback=self.parse_categories,
                meta={"proxy": self.proxy_meta.get("proxy")}
            )

    def parse_categories(self, response):
        category_links = response.xpath('//p[@class="categories-az catTitle national"]/a/@href').getall()
        headers = self.base_headers.copy()
        headers.update(self.proxy_meta.get("headers", {}))

        for link in category_links:
            full_url = response.urljoin(link)
            yield scrapy.Request(
                url=full_url,
                headers=headers,
                callback=self.parse_locations,
                meta={"proxy": self.proxy_meta.get("proxy"), "category_url": full_url}
            )

    def parse_locations(self, response):
        location_urls = response.xpath('//ul[@class="categories-list"]/li/a/@href').getall()
        location_names = response.xpath('//ul[@class="categories-list"]/li/a/text()').getall()

        headers = self.base_headers.copy()
        headers.update(self.proxy_meta.get("headers", {}))

        for url, name in zip(location_urls, location_names):
            full_url = response.urljoin(url)
            yield scrapy.Request(
                url=full_url,
                headers=headers,
                callback=self.parse_location_data,
                meta={
                    "proxy": self.proxy_meta.get("proxy"),
                    "location_name": name.strip(),
                    "location_url": full_url,
                    "category_url": response.meta["category_url"]
                }
            )

    def parse_location_data(self, response):
        location_name = response.meta.get("location_name")
        location_url = response.meta.get("location_url")
        category_url = response.meta.get("category_url")

        # Log to check if the result count is being found
        result_text = response.xpath('//span[@class="resultCount"]/text()').get()
        logging.info(f"Result text: {result_text}")  # Debugging log
        if result_text:
            match = re.search(r'\((\d+)\s+Result', result_text)
            if match:
                num_results = int(match.group(1))
            else:
                num_results = 0
        else:
            num_results = 0

        # Log to check if company IDs are being extracted
        company_ids = self.extract_company_ids(response)
        logging.info(f"Extracted company IDs: {company_ids}")  # Debugging log

        # Output the ids and other relevant data
        for company_id in company_ids:
            logging.info(f"Yielding company ID: {company_id}")  # Debugging log
            yield {
                "listing_id": company_id,
                "location_name": location_name,
                "number_of_locations": num_results,
                "locations_url": location_url
            }

        # Check for next page and continue scraping
        next_page = response.xpath('//a[contains(text(),"Next")]/@href').get()
        if next_page:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(
                url=next_page_url,
                headers=response.request.headers,
                callback=self.parse_location_data,
                meta={"proxy": response.meta.get("proxy"), "location_name": location_name, "location_url": location_url, "category_url": category_url}
            )

    def extract_company_ids(self, response):
        # Use the provided XPath to capture company links by class name
        company_links = response.xpath('//a[@class="listing__name--link listing__link jsListingName"]/@href').getall()
        company_ids = []

        # Extract the company IDs from the href links
        for link in company_links:
            match = re.search(r'/bus/[^/]+/[^/]+/[^/]+/(\d+)\.html', link)
            if match:
                company_ids.append(match.group(1))

        return company_ids

