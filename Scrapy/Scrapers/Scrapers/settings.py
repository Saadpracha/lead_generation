# Scrapers/settings.py

BOT_NAME = "Scrapers"

SPIDER_MODULES = ["Scrapers.spiders"]
NEWSPIDER_MODULE = "Scrapers.spiders"

# Ignore robots.txt
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

# Retry config
RETRY_ENABLED = True
RETRY_TIMES = 5
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 403, 429, 404]
RETRY_PRIORITY_ADJUST = -1

# Enable HTTP/2
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

# Feed export encoding
FEED_EXPORTERS = {
    'csv': 'Scrapers.exporters.Utf8BomCsvItemExporter'
}

# Disable feed exports (pipeline handles output)
FEED_EXPORT_ENABLED = False
FEEDS = {}

# Disable cookies
COOKIES_ENABLED = False

# Slow down to avoid bans
DOWNLOAD_DELAY = 1
DOWNLOAD_TIMEOUT = 30

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"

# Enable custom S3 pipeline
ITEM_PIPELINES = {
    'Scrapers.pipelines.S3OrLocalCsvPipeline': 300,
}

# Default AWS S3 settings
S3_BUCKET = 'bucket-euvdfl'
S3_REGION = 'ca-central-1'