import subprocess
import multiprocessing
import time
import json
import redis
import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx
from scapy.all import sniff, wrpcap
from urllib.parse import urljoin, urlparse
import httpx
from bs4 import BeautifulSoup
import urllib.robotparser
from bloom_filter import BloomFilter
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
from sqlalchemy.exc import IntegrityError
import os
import scrapy
from scrapy.crawler import CrawlerProcess

print("-" * 50)
print("Distributed Web Crawler")
print("-" * 50)

Base=declarative_base()

engine=create_engine('sqlite:///crawled.db', echo=False)

Session = sessionmaker(bind=engine)

class CrawledPage(Base):
    __tablename__ = 'crawled_pages'

    id = Column(Integer, primary_key=True)
    url = Column(String, unique=True)
    title = Column(String)
    content_snippet = Column(Text)
    links_found = Column(Integer)
    timestamp = Column(DateTime, default=datetime.utcnow)
    domain = Column(String)

Base.metadata.create_all(engine)

def scapy_capture():

    print("\n📡 [Scapy] Starting packet capture...")

    try:

        try:

            packets = sniff(
                filter="tcp port 80 or tcp port 443",
                count=30,
                timeout=20
            )

        except:

            print("⚠️ Filter unsupported. Using basic sniff...")

            packets = sniff(
                count=30,
                timeout=20
            )

        wrpcap("crawl_traffic.pcap", packets)

        print("✅ [Scapy] crawl_traffic.pcap saved")

    except Exception as e:

        print(f"⚠️ Scapy disabled: {e}")

class SimpleDHT:

    def __init__(self):
        self.nodes = {i: {} for i in range(4)}

    def store(self, url, data):
        node = hash(url) % 4
        self.nodes[node][url] = data

dht = SimpleDHT()

bloom = BloomFilter(
    max_elements=100000,
    error_rate=0.001
)

redis_host = os.getenv('REDIS_HOST', 'localhost')

print(f"🔗 Connecting to Redis at {redis_host}:6379")

redis_client = redis.Redis(
    host=redis_host,
    port=6379,
    db=0,
    decode_responses=True,
    socket_connect_timeout=5
)

class RealCrawler(scrapy.Spider):

    name = 'real'

    start_urls = [
        "https://en.wikipedia.org/wiki/Main_Page",
        "https://www.python.org",
        "https://www.nasa.gov"
    ]

    custom_settings = {
        'ROBOTSTXT_OBEY': True,
        'CONCURRENT_REQUESTS': 8,
        'DOWNLOAD_DELAY': 1.2,
        'USER_AGENT': 'DistributedCrawler/1.0',
        'DEPTH_LIMIT': 2,
    }

    def parse(self, response):

        if 'text/html' not in response.headers.get(
            'Content-Type',
            ''
        ).decode('utf-8', errors='ignore'):
            return

        yield {
            'url': response.url,
            'title': response.css('title::text').get(default='No Title').strip(),
            'content_snippet': response.text[:700].replace('\n', ' ').strip(),
            'domain': urlparse(response.url).netloc
        }

        for href in response.css('a::attr(href)').getall()[:20]:

            if href.startswith(('http', '/')):
                yield response.follow(href, self.parse)

class DistributedCrawler:

    def __init__(self):

        self.graph = nx.DiGraph()
        self.robot_parsers = {}
        self.last_request = {}

    def respect_robots_and_politeness(self, url):

        domain = urlparse(url).netloc

        if domain not in self.robot_parsers:

            rp = urllib.robotparser.RobotFileParser()

            rp.set_url(f"https://{domain}/robots.txt")

            try:
                rp.read()
            except:
                pass

            self.robot_parsers[domain] = rp

        if not self.robot_parsers[domain].can_fetch("*", url):
            return False

        now = time.time()

        if domain in self.last_request and now - self.last_request[domain] < 1.5:
            time.sleep(1.5)

        self.last_request[domain] = now

        return True

    def run_scapy(self):

        p = multiprocessing.Process(target=scapy_capture)

        p.start()

        return p

    def run_distributed_crawling_worker(self, worker_id):

        print(f"\n🕷️ Worker {worker_id} started")

        crawled = 0

        max_pages = 20

        while crawled < max_pages:

            task = redis_client.lpop('url_queue')

            if not task:
                time.sleep(2)
                continue

            data = json.loads(task)

            url = data["url"]

            if url in bloom:
                continue

            bloom.add(url)

            if not self.respect_robots_and_politeness(url):
                continue

            try:

                resp = httpx.get(
                    url,
                    headers={
                        'User-Agent': f'DistributedCrawlerWorker/{worker_id}'
                    },
                    timeout=10
                )

                if resp.status_code != 200:
                    continue

                if 'text/html' not in resp.headers.get('content-type', ''):
                    continue

                soup = BeautifulSoup(resp.text, 'html.parser')

                title = (
                    soup.title.string.strip()[:200]
                    if soup.title and soup.title.string
                    else "No Title"
                )

                snippet = soup.get_text(
                    separator=' ',
                    strip=True
                )[:650]

                links = []

                for a in soup.find_all('a', href=True)[:25]:

                    full_url = urljoin(url, a['href'])

                    parsed = urlparse(full_url)

                    if parsed.scheme not in ('http', 'https'):
                        continue

                    if any(
                        full_url.lower().endswith(ext)
                        for ext in [
                            '.pdf',
                            '.jpg',
                            '.png',
                            '.zip',
                            '.exe',
                            '.svg',
                            '.mp4',
                            '.mp3'
                        ]
                    ):
                        continue

                    if any(
                        full_url.startswith(x)
                        for x in [
                            'mailto:',
                            'javascript:',
                            '#'
                        ]
                    ):
                        continue

                    links.append(full_url)

                session = Session()

                existing = session.query(CrawledPage).filter_by(url=url).first()

                if existing:
                    session.close()
                    continue
 
                page = CrawledPage(
                    url=url,
                    title=title,
                    content_snippet=snippet,
                    links_found=len(links),
                    domain=urlparse(url).netloc
                )

                try:

                    session.add(page)

                    session.commit()

                except IntegrityError:

                    session.rollback()

                finally:

                    session.close()

                dht.store(url, {"title": title})

                self.graph.add_node(url, title=title)

                for link in links[:10]:
                    self.graph.add_edge(url, link)

                for link in links:

                    if link not in bloom:

                        redis_client.rpush(
                            'url_queue',
                            json.dumps({
                                "url": link,
                                "priority": data["priority"] + 1
                            })
                        )

                crawled += 1

                print(
                    f"✅ Worker {worker_id} "
                    f"[{crawled}/{max_pages}] {url}"
                )

            except Exception as e:

                print(f"⚠️ Worker {worker_id}: {url}: {e}")

    def run_scrapy(self):

        print("\n🕸️ [Scrapy] Running...")

        process = CrawlerProcess({
            'FEEDS': {
                'scrapy_output.json': {
                    'format': 'json'
                }
            },
            'LOG_LEVEL': 'ERROR',
            'DEPTH_LIMIT': 2
        })

        process.crawl(RealCrawler)

        process.start()

        print("✅ Scrapy completed")

    def run_analysis(self):

        print("\n📊 [Analysis] Running...")

        df = pd.read_sql(
            "SELECT * FROM crawled_pages",
            engine
        )

        df.to_csv(
            'crawled_data.csv',
            index=False
        )

        df.to_json(
            'crawled_data.json',
            orient='records',
            indent=2
        )

        plt.figure(figsize=(12, 6))

        df['domain'].value_counts().head(10).plot(kind='bar')

        plt.title('Pages Crawled per Domain')

        plt.savefig('domain_distribution.png')

        plt.close()

    def run_shell_tools(self):

        print("\n🛠️ [Shell Tools] Running...")

        subprocess.run(
            "curl -I -A 'Crawler' "
            "https://en.wikipedia.org/wiki/Main_Page "
            "> headers.log 2>&1",
            shell=True
        )

        subprocess.run(
            "wget --spider -r -l 1 "
            "https://www.python.org "
            "2> wget.log",
            shell=True
        )

        subprocess.run(
            "awk '/HTTP/ {print $0}' headers.log "
            "| sed 's/HTTP/Status:/' "
            "> processed_headers.log",
            shell=True
        )

        subprocess.run(
            "jq '.[0:10] | .[].title' crawled_data.json "
            "> top_titles.txt 2>/dev/null || echo 'jq done'",
            shell=True
        )

        print("✅ Shell tools completed")

    def run_all(self):

        seeds = [
            "https://en.wikipedia.org/wiki/Main_Page",
            "https://www.python.org",
            "https://www.nasa.gov"
        ]

        for url in seeds:

            redis_client.rpush(
                'url_queue',
                json.dumps({
                    "url": url,
                    "priority": 0
                })
            )

        scapy_proc = self.run_scapy()

        print("\n🚀 Starting distributed workers...")

        workers = []

        num_workers = 4

        for i in range(num_workers):

            p = multiprocessing.Process(
                target=self.run_distributed_crawling_worker,
                args=(i,)
            )

            p.start()

            workers.append(p)

        for p in workers:
            p.join()

        self.run_scrapy()

        self.run_analysis()

        self.run_shell_tools()

        scapy_proc.join()

        print("\n🎉 COMPLETED SUCCESSFULLY!")

if __name__ == "__main__":

    crawler = DistributedCrawler()

    crawler.run_all()
