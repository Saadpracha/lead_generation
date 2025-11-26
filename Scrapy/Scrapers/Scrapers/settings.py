# Scrapers/settings.py
import os
from dotenv import load_dotenv

# Load environment variables from a .env file if present
load_dotenv()
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
DOWNLOAD_DELAY = 0.2
DOWNLOAD_TIMEOUT = 30

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"

# Enable custom S3 pipeline
ITEM_PIPELINES = {
    'Scrapers.pipelines.S3OrLocalCsvPipeline': 300,
}

# Keep existing S3 defaults you already have:
S3_BUCKET = 'bucket-euvdfl'
S3_REGION = 'ca-central-1'



# --- Mailgun Email Notification Settings (from environment) ---
# Examples in .env:
# MAILGUN_API_KEY=key-xxxx
# MAILGUN_DOMAIN=mail.example.com
# MAILGUN_FROM=no-reply@mail.example.com
# NOTIFY_EMAILS=ops@example.com,dev@example.com
MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY")
MAILGUN_DOMAIN = os.getenv("MAILGUN_DOMAIN")
MAILGUN_BASE_URL = os.getenv("MAILGUN_BASE_URL", "https://api.mailgun.net/v3")
MAILGUN_FROM = os.getenv("MAILGUN_FROM") or (f"no-reply@{MAILGUN_DOMAIN}" if MAILGUN_DOMAIN else None)

# Default: no email notifications unless explicitly passed
NOTIFY_EMAILS = os.getenv("NOTIFY_EMAILS")
NOTIFY_EMAIL = os.getenv("NOTIFY_EMAIL")
