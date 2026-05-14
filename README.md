# Distributed Web Crawler

A distributed web crawler built using Python, Redis/Valkey, multiprocessing, Scrapy, BeautifulSoup, SQLAlchemy, and NetworkX.

The project demonstrates distributed crawling, concurrent processing, web scraping, database storage, graph analysis, and Linux shell tool integration.

---

# Features

* Distributed crawling using Redis/Valkey
* Multiprocessing worker architecture
* BeautifulSoup and Scrapy based crawling
* Bloom Filter URL deduplication
* robots.txt compliance
* SQLite database storage
* CSV and JSON export
* Graph analysis using NetworkX
* Shell tool integration using curl, wget, awk, sed, and jq
* Optional packet capture using Scapy

---

# Technologies Used

* Python
* Redis / Valkey
* Multiprocessing
* Scrapy
* BeautifulSoup
* SQLAlchemy
* SQLite
* NetworkX
* matplotlib
* Scapy

---

# Project Architecture

```text
                 +----------------+
                 | Seed URLs      |
                 +--------+-------+
                          |
                          v
                 +----------------+
                 | Redis Queue    |
                 | url_queue      |
                 +--------+-------+
                          |
        ---------------------------------------
        |              |             |         |
        v              v             v         v
    Worker 0       Worker 1      Worker 2  Worker 3
        |              |             |         |
        ---------------------------------------
                          |
                          v
                 Crawl & Parse Pages
                          |
                          v
                SQLite Database Storage
                          |
                          v
                 Analysis + Visualization
```

---

# Installation

## Clone Repository

```bash
git clone https://github.com/yourusername/distributed-web-crawler.git
cd distributed-web-crawler
```

## Install Dependencies

```bash
pip install redis pandas matplotlib networkx scapy httpx beautifulsoup4 sqlalchemy scrapy bloom-filter2
```

## Start Valkey (Arch Linux)

```bash
sudo systemctl start valkey
```

## Run Project

```bash
python main.py
```

---

# Outputs

```text
crawled.db
crawled_data.csv
crawled_data.json
domain_distribution.png
scrapy_output.json
headers.log
processed_headers.log
wget.log
top_titles.txt
```

Optional:

```text
crawl_traffic.pcap
```

---

# Folder Structure

```text
Web-Crawler/
│
├── main.py
├── requirements.txt
├── README.md
├── .gitignore
│
├── data/
├── outputs/
└── screenshots/
```

---

# License

MIT License
