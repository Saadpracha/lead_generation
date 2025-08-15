BOT_NAME = "yellowpages"

SPIDER_MODULES = ["yellowpages.spiders"]
NEWSPIDER_MODULE = "yellowpages.spiders"

# Ignore robots.txt (YellowPages will block scraping if you obey it)
ROBOTSTXT_OBEY = False


# Allow redirects to Akamai CDN IPs
SPIDER_MIDDLEWARES = {
    'scrapy.spidermiddlewares.offsite.OffsiteMiddleware': None
}

# Enable proxy usage
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 750,
    'scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware': None,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 550,
}

# Retry config: enable and try up to 3 times
RETRY_ENABLED = True
RETRY_TIMES = 5
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 403, 429, 404]

RETRY_PRIORITY_ADJUST = -1


# Enable HTTP/2 if needed
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

# Feed export encoding
FEED_EXPORTERS = {
    'csv': 'yellowpages.exporters.Utf8BomCsvItemExporter'
}


# Disable cookies to avoid tracking
COOKIES_ENABLED = False

# Respect but slow down to avoid bans
DOWNLOAD_DELAY = 1


DOWNLOAD_TIMEOUT = 30


REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"