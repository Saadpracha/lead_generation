This Project uses the Scrapy framework for Data Extraction. 
Things to Remeber for now:
I have integrated only proxy inside the code for prototype, will integrate the proxies files later.
Your location must be where the scrapy.cfg file is located to run the code..... 
For changing the directory you must use (cd yellowpages) 
While running the command must use the paramaters of what and where it's detail is below

---

## ✅ `README.md`

```markdown
# YellowPages Scraper (Scrapy Project)

This project is a Scrapy spider designed to scrape business listings from [YellowPages.ca](https://www.yellowpages.ca), using proxy support and configurable search queries (`what` and `where`).

---

## 📁 Project Structure

```

yellowpages/
├── scrapy.cfg
├── requirements.txt
├── README.md
├── venv/               
├── yellowpages/
│   ├── **init**.py
│   ├── items.py
│   ├── middlewares.py
│   ├── pipelines.py
│   ├── settings.py
│   └── spiders/
│       └── yp.py

````

---

## 🧰 Prerequisites

- Python 3.10 or higher
- `pip` (Python package manager)
- `git` (optional, for version control)

---

## 🐍 1. Create & Activate a Virtual Environment

### Linux / macOS

```bash
python3 -m venv venv
source venv/bin/activate
````

### Windows

```bash
python -m venv venv
venv\Scripts\activate
```

---

## 📦 2. Install Dependencies

Install all required Python packages using `requirements.txt`:

```bash
pip install -r requirements.txt
```

---

## 🕷️ 3. Run the YellowPages Spider

To start scraping, run:

```bash
scrapy crawl yellowpages -a what='YOUR_SEARCH_TERM' -a where='YOUR_LOCATION'
```

### Example:

```bash
scrapy crawl yellowpages -a what='plumber' -a where='Toronto ON'
```

> Both `what` and `where` are required arguments.

---

## 🌐 4. Proxy Configuration

This spider uses a proxy by default. The proxy is configured in the spider file itself:

```python
proxy_host = "23.95.150.145"
proxy_port = "6114"
proxy_user = "nhjbaddy"
proxy_password = "zmkmfl18hq36"
```

To change these, modify the `yp.py` spider directly.

---

## 📤 5. Exporting Output (Optional)

You can export the scraped data to a file using the `-o` flag:

### JSON

```bash
scrapy crawl yellowpages -a what='dentist' -a where='Vancouver BC' -o output.json
```

### CSV

```bash
scrapy crawl yellowpages -a what='dentist' -a where='Vancouver BC' -o output.csv
```

---

## 🚫 6. Exit Virtual Environment (When Done)

```bash
exit
```

---

## 🛠️ 7. Troubleshooting

* Make sure your internet connection is stable.
* Ensure `what` and `where` are properly quoted if they include spaces.


## 7. Added the file logic

I have added the file for input data , What and where values are added. For running the files the input is 

```bash
scrapy crawl yellowpages -a input_file=input.xlsx -o output2.csv 
```

## 8. Saving the logs

If you want to save the logs, Use this prompt

```bash
scrapy crawl yellowpages -a input_file=input.xlsx -o output.csv --logfile output.log
```

## 9. Extra things

I have use the Fingerprint logic and Retry Logic in settings.py. Also for saving the encoding = "utf-8-sig" by making the exporters.py and calling it in settings.py

## 10. Adding Proxies 

So I have made the logic for proxies in a code, i.e when the code is run it will take the first one proxy and run through it, when some error like status code other than 200 will come it will start shifting to next proxy and start requesting.

## 11. Summary Logic 

when ever the code is executed it will make the summary file in json format by it's own so to make the summary file with your desired name just add this in prompt

```bash
scrapy crawl yellowpages -a what=what.xlsx -a where=where.xlsx -a summary_file=detail_output_summary.json -o detail_output.csv  --logfile detail_output.log
```
Now in above promt user will be getting the summary file, log file and output file 