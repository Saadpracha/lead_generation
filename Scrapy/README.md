
# 🟡 YellowPages.ca Business Listings Scraper (Canada)

A **Scrapy**-based spider to scrape business listings from [YellowPages.ca](https://www.yellowpages.ca/). This scraper supports **proxy rotation**, **pagination**, **category filtering**, **deduplication**, and customizable output and summary reporting.

---

## 📁 Project Structure

```
Scraper/
│
├── Scrapers/
│   ├── spiders/
│   │   └── yellowpages_canada.py       # Main spider file for scraping YP Canada
│   ├── exporters.py                    # Custom exporter for UTF-8-SIG encoded CSV
│   ├── settings.py                     # Scrapy settings (proxy, retry, etc.)
│
├── proxies.json                        # Proxy list (IP:Port or IP:Port:User:Pass)
├── what.xlsx                           # Search keywords (e.g., "Plumber", "Dentist")
├── where.xlsx                          # Search locations (e.g., "Toronto", "Vancouver")
└── scrapy.cfg                          # Scrapy configuration file
└── imp_data/
    └── YP_Canada/
        └── output/
            └── [dir_name]/
                ├── [output_file].csv   # Scraped results
                └── [summary].json      # Run summary JSON file
```

---

## ▶️ How to Run the Spider

Navigate to the project root and run the spider with required arguments:

```bash
scrapy crawl yellowpages_canada \
  -a what=what.xlsx \
  -a where=where.xlsx \
  -a dir_name=SOLAR \
  -a output_file=yp_canada_solar_ON.csv \
  -a summary=yp_canada_solar_ON.json \
  -a source=ON-Solar \
  -a category_matching=yes
```

### ✅ Required Arguments

| Argument      | Description                                                                         |
| ------------- | ----------------------------------------------------------------------------------- |
| `what`        | Path to Excel file (`.xlsx`) with a `what` column (business types, e.g., "Plumber") |
| `where`       | Path to Excel file (`.xlsx`) with a `where` column (locations, e.g., "Toronto")     |
| `dir_name`    | Folder name inside `imp_data/YP_Canada/output/` where outputs will be saved         |
| `output_file` | Output CSV file name (must end with `.csv`)                                         |

### 🔁 Optional Arguments

| Argument            | Description                                                                                          |
| ------------------- | ---------------------------------------------------------------------------------------------------- |
| `summary`           | Output filename for JSON summary report (default: `summary.json`)                                    |
| `source`            | Source tag for labeling the dataset (e.g., project or campaign name)                                 |
| `category_matching` | Set to `yes` to **exclude** listings whose categories don't match the `what` keyword (default: `no`) |

> **Note:** Output files are automatically saved to:
> `imp_data/YP_Canada/output/[dir_name]/[output_file].csv`
> Summary will be saved as:
> `imp_data/YP_Canada/output/[dir_name]/[summary].json`

---

## ✨ Key Features

### ✅ Proxy Rotation

Reads proxies from `proxies.json` and rotates them on request failures. Supports both IP-only and authenticated proxies:

* `IP:PORT`
* `IP:PORT:USER:PASS`

### ✅ Excel Input Parsing

Supports `.xlsx` input files for both `what` and `where`, extracting data from columns named exactly:

* `what`
* `where`

### ✅ Pagination Support

Follows "Next" page links to scrape all results for each search term and location.

### ✅ Deduplication

Skips duplicate listings based on unique `listing_id`, `URL`, or fallback `name_phone` combination.

### ✅ Category Matching

If `category_matching` is set to `yes`, only listings whose categories match the search term (`what`) will be included.

### ✅ Profile Enrichment

Fetches individual business profile pages for:

* Phone numbers (primary and additional)
* Website (if available)
* Address (split into components)
* Categories
* Additional metadata (e.g., “Sponsored” flag)

### ✅ Summary Report

After the spider finishes, a summary JSON file is generated containing:

* Total and unique listings
* Errors and excluded records
* Runtime statistics (start/end times, duration)
* Input combinations
* Output paths

---

## 📦 Output Format

Output is saved as a **UTF-8 CSV file**.

| Field Name       | Description                                                 |
| ---------------- | ----------------------------------------------------------- |
| `listing_id`     | YellowPages internal listing ID                             |
| `company`        | Business name                                               |
| `phone`          | Primary phone number                                        |
| `all_phones`     | Comma-separated list of all phone numbers                   |
| `email`          | Email address (if available via structured data)            |
| `website`        | Business website URL                                        |
| `address`        | Street address                                              |
| `city`           | City                                                        |
| `state`          | Province or state                                           |
| `postal_code`    | Postal code                                                 |
| `full_address`   | Full formatted address (as seen on the page)                |
| `country`        | Country code (always `CA` for Canada)                       |
| `what`           | Search keyword used for this entry                          |
| `where`          | Location used for this entry                                |
| `scraper_source` | Name of the spider (`yellowpages_canada`)                   |
| `source_url`     | Direct link to the business listing                         |
| `note`           | Additional info (e.g., “Sponsored” listings)                |
| `category`       | Comma-separated list of categories assigned to the business |
| `source`         | User-defined source value passed via `-a source=...`        |

---

## 🛠 Troubleshooting

* Ensure `proxies.json`, `what.xlsx`, and `where.xlsx` are in the project root.
* Input files **must** include `what` and `where` columns.
* Use `--logfile run.log` to save logs for debugging.
* Make sure the output directory and filenames use valid characters only (alphanumeric, `-`, `_`, `.`).
* If running into issues with proxy rotation, check the proxies file format.

---

