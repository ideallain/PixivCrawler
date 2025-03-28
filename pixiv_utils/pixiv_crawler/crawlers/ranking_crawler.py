import concurrent.futures as futures
import datetime
import time
import random
import re
import json
import os
from typing import Set, Union, List

import tqdm
from pixiv_utils.pixiv_crawler.collector import Collector, collect, selectRanking, selectRankingPage, selectRecommends
from pixiv_utils.pixiv_crawler.config import download_config, ranking_config, user_config
from pixiv_utils.pixiv_crawler.downloader import Downloader
from pixiv_utils.pixiv_crawler.utils import printInfo
from kafka import KafkaProducer
from bs4 import BeautifulSoup
from ebooklib import epub
import requests
from requests.models import Response
# Singleton KafkaProducer cache
_producer = None

def getKafkaProducer() -> KafkaProducer:
    """
    Get or create a KafkaProducer instance (singleton pattern)
    """
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
        )
    return _producer

def metaPreloadDataToEpub(response: Response, epub_filename: str) -> None:
    """
    Convert Pixiv novel data to EPUB format
    1. Extract meta preload data from HTML
    2. Parse JSON content
    3. Generate EPUB from novel content
    """
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find meta tag with preload data
    meta_tag = soup.find('meta', attrs={'name': 'preload-data', 'id': 'meta-preload-data'})
    if not meta_tag:
        print("[INFO] No meta-preload-data tag found.")
        return
    
    # Get content attribute (JSON string)
    json_str = meta_tag.get("content")
    if not json_str:
        print("[INFO] meta-preload-data tag has no content attribute or is empty.")
        return

    # Parse JSON
    try:
        parsed_data = json.loads(json_str)
    except json.JSONDecodeError as e:
        print("[ERROR] Content in meta-preload-data is not valid JSON:", e)
        return

    # Get novel data
    novel_data = parsed_data.get("novel")
    if not novel_data or not isinstance(novel_data, dict):
        print("[INFO] No 'novel' object found in JSON.")
        return

    # Get first novel ID
    first_id = next(iter(novel_data))
    novel_info = novel_data.get(first_id, {})
    if not novel_info:
        print("[INFO] Novel information is empty.")
        return

    # Get novel content
    content_text = novel_info.get("content")
    if not content_text:
        print("[INFO] No 'content' field in novel_info.")
        return

    # Create EPUB
    book = epub.EpubBook()
    book.set_identifier("novel-epub-id")
    book.set_language("zh")
    title = novel_info.get("title", "Untitled Novel")
    author = novel_info.get("userName", "PixivCrawler")

    book.set_title(title)
    book.add_author(author)

    # Create chapter with content
    chapter = epub.EpubHtml(title="Content", file_name="chapter1.xhtml", lang="zh")
    chapter.content = content_text

    book.add_item(chapter)

    # Set TOC and navigation
    book.toc = (epub.Link('chapter1.xhtml', 'Content', 'chapter1'),)
    book.add_item(epub.EpubNcx())
    book.add_item(epub.EpubNav())

    # Reading order
    book.spine = ['nav', chapter]

    # Write EPUB file
    epub.write_epub(epub_filename, book, {})
    print(f"[INFO] EPUB file generated: {epub_filename}")


def fetchWorkDataAndSendToKafka(work_id: str, work_type: str = "novel", topic: str = "pixiv_works"):
    """
    Fetch artwork data by ID and send to Kafka
    """
    # Determine URL based on work type
    if work_type == "novel":
        url = f"https://www.pixiv.net/novel/show.php?id={work_id}"
    else:  # illust
        url = f"https://www.pixiv.net/artworks/{work_id}"
    
    headers = {
        "Referer": f"https://www.pixiv.net/{work_type}/",
        "x-requested-with": "XMLHttpRequest",
        "COOKIE": user_config.cookie or "",
    }

    try:
        # Request data
        printInfo(f"[INFO] Fetching data for work {work_id}...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except Exception as e:
        printInfo(f"[ERROR] Error fetching work ({work_id}): {e}")
        return False

    # Parse page and extract metadata
    soup = BeautifulSoup(response.text, 'html.parser')
    meta_tag = soup.find('meta', attrs={'name': 'preload-data', 'id': 'meta-preload-data'})
    
    if not meta_tag:
        printInfo(f"[INFO] No meta-preload-data tag found for work {work_id}")
        return False

    # Get JSON string
    json_str = meta_tag.get("content")
    if not json_str:
        printInfo(f"[INFO] meta-preload-data tag has no content or is empty for work {work_id}")
        return False

    try:
        # Parse JSON data
        parsed_data = json.loads(json_str)
        
        # Add additional information
        message_data = {
            "work_id": work_id,
            "work_type": work_type,
            "fetch_time": datetime.datetime.now().isoformat(),
            "data": parsed_data
        }
        
        # Get Kafka producer
        producer = getKafkaProducer()
        
        # Serialize and send to Kafka
        json_data = json.dumps(message_data, ensure_ascii=False).encode('utf-8')
        producer.send(topic, json_data)
        print(json_data)
        
        # Ensure message is sent
        producer.flush()
        
        printInfo(f"[INFO] Data for work {work_id} sent to Kafka topic {topic}")
        return True
    except json.JSONDecodeError as e:
        printInfo(f"[ERROR] Data for work {work_id} is not valid JSON: {e}")
    except Exception as e:
        printInfo(f"[ERROR] Error sending data for work {work_id} to Kafka: {e}")
    
    return False

def processWorkIdsAndSendToKafka(ids_file: str, work_type: str = "novel", topic: str = "pixiv_works", num_threads: int = None):
    """
    Process work IDs from JSON file and send to Kafka in parallel
    """
    # Use configured thread count or specified count
    threads = num_threads if num_threads is not None else download_config.num_threads
    
    try:
        # Read work IDs list
        with open(ids_file, "r", encoding="utf-8") as f:
            work_ids = json.load(f)
        
        if not isinstance(work_ids, list):
            printInfo(f"[ERROR] Content of {ids_file} is not a valid ID list")
            return
        
        printInfo(f"[INFO] Read {len(work_ids)} work IDs from {ids_file}")
        
        # Parallel processing
        printInfo(f"[INFO] Starting parallel processing of {len(work_ids)} works and sending to Kafka using {threads} threads...")
        
        success_count = 0
        fail_count = 0
        
        with futures.ThreadPoolExecutor(threads) as executor:
            with tqdm.tqdm(total=len(work_ids), desc=f"Sending {work_type} to Kafka") as pbar:
                future_list = [
                    executor.submit(fetchWorkDataAndSendToKafka, work_id, work_type, topic)
                    for work_id in work_ids
                ]
                
                for future in futures.as_completed(future_list):
                    result = future.result()
                    if result:
                        success_count += 1
                    else:
                        fail_count += 1
                    pbar.update(1)
        
        printInfo(f"[INFO] All work data processing complete: {success_count} successful, {fail_count} failed")
        
    except Exception as e:
        printInfo(f"[ERROR] Error processing work ID list: {e}")

def fetchNovelAndGenerateEpub(novel_id: str):
    """
    Fetch novel by ID and generate EPUB
    """
    # Pixiv novel reading URL
    url = f"https://www.pixiv.net/novel/show.php?id={novel_id}"

    headers = {
        "Referer": "https://www.pixiv.net/novel/",
        "x-requested-with": "XMLHttpRequest",
        "COOKIE": user_config.cookie,  # If Pixiv login is required
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        # Generate EPUB filename based on novel ID
        epub_filename = f"novel_{novel_id}.epub"
        metaPreloadDataToEpub(response, epub_filename)
    except Exception as e:
        printInfo(f"Error fetching novel {novel_id}: {e}")

def fetchNovelAndSaveToJson(novel_id: str, output_dir: str = "novels_json"):
    """
    Fetch novel by ID and save raw preload data to JSON file
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Construct output file path
    output_file = os.path.join(output_dir, f"novel_{novel_id}.json")
    
    # Skip if file already exists
    if os.path.exists(output_file):
        printInfo(f"[INFO] JSON file for novel {novel_id} already exists, skipping")
        return
    
    # Pixiv novel reading URL
    url = f"https://www.pixiv.net/novel/show.php?id={novel_id}"
    headers = {
        "Referer": "https://www.pixiv.net/novel/",
        "x-requested-with": "XMLHttpRequest",
        "COOKIE": user_config.cookie or "",
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except Exception as e:
        printInfo(f"[WARN] Error fetching novel page ({novel_id}): {e}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    meta_tag = soup.find('meta', attrs={'name': 'preload-data', 'id': 'meta-preload-data'})
    if not meta_tag:
        printInfo(f"[INFO] No meta-preload-data tag found for novel {novel_id}")
        return

    # Get content attribute (raw JSON string)
    json_str = meta_tag.get("content")
    if not json_str:
        printInfo(f"[INFO] meta-preload-data tag has no content or is empty for novel {novel_id}")
        return

    try:
        # Parse JSON to ensure format is correct
        parsed_data = json.loads(json_str)
        
        # Save parsed data to file
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(parsed_data, f, indent=4, ensure_ascii=False)
        
        printInfo(f"[INFO] Data for novel {novel_id} saved to {output_file}")
    except json.JSONDecodeError as e:
        printInfo(f"[ERROR] Data for novel {novel_id} is not valid JSON: {e}")
    except Exception as e:
        printInfo(f"[ERROR] Error saving data for novel {novel_id}: {e}")

def processNovelIdsFromFile(ids_file: str, output_dir: str = "novels_json", num_threads: int = None):
    """
    Process novel IDs from JSON file and save each novel's data as individual JSON file
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Use configured thread count or specified count
    threads = num_threads if num_threads is not None else download_config.num_threads
    
    try:
        # Read novel IDs list
        with open(ids_file, "r", encoding="utf-8") as f:
            novel_ids = json.load(f)
        
        if not isinstance(novel_ids, list):
            printInfo(f"[ERROR] Content of {ids_file} is not a valid ID list")
            return
        
        printInfo(f"[INFO] Read {len(novel_ids)} novel IDs from {ids_file}")
        
        # Parallel processing
        printInfo(f"[INFO] Starting parallel processing of {len(novel_ids)} novels using {threads} threads...")
        
        with futures.ThreadPoolExecutor(threads) as executor:
            with tqdm.tqdm(total=len(novel_ids), desc="Fetching novel data") as pbar:
                future_list = [
                    executor.submit(fetchNovelAndSaveToJson, novel_id, output_dir)
                    for novel_id in novel_ids
                ]
                
                for future in futures.as_completed(future_list):
                    # Update progress bar
                    pbar.update(1)
        
        printInfo(f"[INFO] All novel data fetching complete, saved to {output_dir} directory")
        
    except Exception as e:
        printInfo(f"[ERROR] Error processing novel ID list: {e}")


class RankingCrawler:
    def __init__(self, capacity: float = 1024, recommends_tags: List[str] = None, max_pages = 5):
        """
        Initialize RankingCrawler to download artworks from ranking or recommendations
        
        Args:
            capacity (float): Flow capacity in MB, default 1024
            recommends_tags (List[str]): Tags for recommendations mode
            max_pages (int): Maximum pages to fetch per tag, default 5
        """
        self.date = ranking_config.start_date
        self.range = ranking_config.range
        self.mode = ranking_config.mode

        self.is_recommends_mode = recommends_tags is not None
        # Config only for recommends mode
        if self.is_recommends_mode:
            self.recommends_tags = recommends_tags
            self.max_pages = max_pages

        # Configure URL template for ranking mode
        if not self.is_recommends_mode:
            self.content = ranking_config.content_mode
            assert self.mode in ranking_config.ranking_modes, f"Invalid mode: {self.mode}"
            assert self.content in ranking_config.content_modes, f"Invalid content mode: {self.content}"

            if self.content == "novel": 
                base_url = "https://www.pixiv.net/novel/ranking.php?"
            else:
                base_url = "https://www.pixiv.net/ranking.php?"
            self.url_template = base_url + "&".join(
                [
                    f"mode={self.mode}",
                    f"content={self.content}",
                    "date={}",
                    "p={}",
                    "format=json",
                ]
            )

        self.downloader = Downloader(capacity)
        self.collector = Collector(self.downloader)

    def _collect(self, artworks_per_json: int = 50):
        """Main collection method that dispatches to appropriate sub-method"""
        if self.is_recommends_mode:
            self._collect_recommends()
        else: 
            self._collect_ranking(artworks_per_json)

    def _collect_recommends(self):
        """Collect novel data from recommendation pages for specified tags"""
        printInfo(f"===== Start collecting recommendations for {len(self.recommends_tags)} tags =====")
        
        # Store all novel data
        all_novel_data = []
        all_novel_ids = set()
        
        # Build all request URLs
        urls = set()
        additional_headers = []
        
        for tag in self.recommends_tags:
            encoded_tag = requests.utils.quote(tag)
            for page in range(1, self.max_pages + 1):
                url = f"https://www.pixiv.net/ajax/search/novels/{encoded_tag}?word={encoded_tag}&order=date_d&mode=all&p={page}&csw=0&s_mode=s_tag&gs=1&lang=zh_tw&version=e11a103b148c138660cbd4085334f4e6da87a9f1"
                urls.add(url)
                
                # Add headers for each URL
                referer_value = f"https://www.pixiv.net/tags/{encoded_tag}/novels"
                additional_headers.append({
                    "Referer": referer_value,
                    "x-requested-with": "XMLHttpRequest",
                    "COOKIE": user_config.cookie,
                })
        
        printInfo(f"{download_config.num_threads} threads are used to collect recommendations")
        
        with futures.ThreadPoolExecutor(download_config.num_threads) as executor:
            with tqdm.tqdm(total=len(urls), desc="Collecting recommendations") as pbar:
                # Submit all tasks
                futures_list = [
                    executor.submit(collect, url, selectRecommends, header)
                    for url, header in zip(urls, additional_headers)
                ]
                
                # Collect results
                for future in futures.as_completed(futures_list):
                    body_data = future.result()
                    if body_data is not None and "novel" in body_data and "data" in body_data["novel"]:
                        novels_data = body_data["novel"]["data"]
                        
                        # Add to total dataset
                        all_novel_data.extend(novels_data)
                        
                        # Extract novel IDs
                        for novel in novels_data:
                            if "novelId" in novel:
                                all_novel_ids.add(novel["novelId"])
                            elif "latestEpisodeId" in novel:
                                all_novel_ids.add(novel["latestEpisodeId"])
                    
                    pbar.update(1)
        
        # Save complete data to JSON file
        with open("novels_from_recommends.json", "w", encoding="utf-8") as f:
            json.dump(all_novel_data, f, indent=4, ensure_ascii=False)
        
        # Save ID list
        self._save_to_json(all_novel_ids, "novel_ids_from_recommends.json")
        
        printInfo(f"Collected {len(all_novel_data)} novel entries with {len(all_novel_ids)} unique IDs")
        
        # Add to collector for further processing
        self.collector.add(all_novel_ids)
        
        printInfo(f"===== Collection complete =====")

    def _process_tag_page(self, tag: str, page: int):
        """
        Process a single tag page and extract novel and series IDs
        
        Args:
            tag (str): Tag name
            page (int): Page number
            
        Returns:
            tuple: (novel_ids, series_ids, next_pages)
        """
        encoded_tag = requests.utils.quote(tag)
        url = f"https://www.pixiv.net/ajax/search/novels/{encoded_tag}?word={encoded_tag}&order=date_d&mode=all&p={page}&csw=0&s_mode=s_tag_full&gs=1&lang=zh_tw&version=e11a103b148c138660cbd4085334f4e6da87a9f1"
        headers = {
            "Referer": "https://www.pixiv.net/",
            "x-requested-with": "XMLHttpRequest",
            "COOKIE": user_config.cookie or "",
        }
        
        try:
            # Add small random delay for rate limiting
            time.sleep(random.uniform(0.5, 2.0))
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            # Extract IDs using selector function
            novel_ids, series_ids, has_next = selectRecommends(response)
            
            # Determine if more pages to fetch
            next_pages = []
            if has_next and page < 10:  # Limit to max 10 pages per tag
                next_pages.append(page + 1)
                
            return novel_ids, series_ids, next_pages
            
        except Exception as e:
            printInfo(f"Error processing tag page {tag}, page {page}: {e}")
            return set(), set(), []

    def _save_to_json(self, ids: Set[str], filename: str):
        """Save set of IDs to JSON file"""
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(list(ids), f, indent=4, ensure_ascii=False)
        printInfo(f"Saved {len(ids)} IDs to {filename}")

    def _collect_ranking(self, artworks_per_json: int = 50):
        """Collect artworks from ranking pages"""
        num_page = (ranking_config.num_artwork - 1) // artworks_per_json + 1  # ceil

        def addDate(current: datetime.date, days):
            return current + datetime.timedelta(days)

        content = f"{self.mode}:{self.content}"
        printInfo(f"===== Start collecting {content} ranking =====")
        printInfo(
            "From {} to {}".format(
                self.date.strftime("%Y-%m-%d"),
                addDate(self.date, self.range - 1).strftime("%Y-%m-%d"),
            )
        )

        # Build all request URLs
        urls: Set[str] = set()
        for _ in range(self.range):
            for i in range(num_page):
                urls.add(self.url_template.format(self.date.strftime("%Y%m%d"), i + 1))
            self.date = addDate(self.date, 1)

        printInfo(f"{download_config.num_threads} threads are used to collect {content} ranking")

        with futures.ThreadPoolExecutor(download_config.num_threads) as executor:
            with tqdm.trange(len(urls), desc=f"Collecting {self.content} ids") as pbar:
                additional_headers = []
                for url in urls:
                    # Extract Referer from URL
                    referer_match = re.search("(.*)&p", url)
                    referer_value = referer_match.group(1) if referer_match else url
                    additional_headers.append({
                        "Referer": referer_value,
                        "x-requested-with": "XMLHttpRequest",
                        "COOKIE": user_config.cookie,
                    })

                # Choose selector based on content type
                if self.content == "novel":
                    printInfo(f"Collecting novel ranking page...")
                    selector_func = selectRankingPage
                else:
                    printInfo(f"Collecting illustration ranking page...")
                    selector_func = selectRanking

                # Submit all tasks
                futures_list = [
                    executor.submit(collect, url, selector_func, header)
                    for url, header in zip(urls, additional_headers)
                ]

                # Collect results
                all_image_ids = set()
                for future in futures.as_completed(futures_list):
                    image_ids = future.result()
                    if image_ids is not None:
                        # Add IDs to total set
                        all_image_ids.update(image_ids)
                        printInfo(f"{all_image_ids}")
                    pbar.update()

        # Save all IDs to JSON file
        if self.content == "novel":
            file_path = "novel_image_ids.json"
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(list(all_image_ids), f, indent=4, ensure_ascii=False)
            printInfo(f"All novel IDs saved to {file_path}")

            # Save novel data to JSON files
            output_dir = "novels_json"
            printInfo(f"Starting parallel fetching of {len(all_image_ids)} novels and saving as JSON files...")
            
            os.makedirs(output_dir, exist_ok=True)
            with futures.ThreadPoolExecutor(download_config.num_threads) as executor:
                future_list3 = [
                    executor.submit(fetchNovelAndSaveToJson, novel_id, output_dir)
                    for novel_id in all_image_ids
                ]
                with tqdm.tqdm(total=len(all_image_ids), desc="Fetching novel JSON") as pbar:
                    for _ in futures.as_completed(future_list3):
                        pbar.update(1)
            
            printInfo(f"All novel data saved to {output_dir} directory")
            
        # Add IDs to collector
        self.collector.add(all_image_ids)

        printInfo(f"===== Collect {content} ranking complete =====")

    def run(self) -> Union[Set[str], float]:
        """
        Run the ranking or recommend crawler
        
        Returns:
            Union[Set[str], float]: Artwork URLs or download traffic usage
        """
        print("[DEBUG] RankingCrawler.run() is called")
        self._collect()
        print("[DEBUG] after self._collect()")
        # self.collector.collect()  # Commented out in original code
        return self.downloader.download()



if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Pixiv Crawler with Kafka Support")
    parser.add_argument("--ids-file", help="Read work IDs from specified JSON file")
    parser.add_argument("--output-dir", default="novels_json", help="Output directory, default: novels_json")
    parser.add_argument("--threads", type=int, help="Thread count, default: use configured count")
    parser.add_argument("--ranking", action="store_true", help="Fetch works from ranking")
    parser.add_argument("--work-type", default="novel", choices=["novel", "illust"], 
                        help="Work type: novel or illust, default: novel")
    parser.add_argument("--kafka", action="store_true", help="Send data to Kafka")
    parser.add_argument("--topic", default="pixiv_works", help="Kafka topic name, default: pixiv_works")
    parser.add_argument("--work-id", help="Single work ID to fetch and process")
    
    args = parser.parse_args()
    
    # Process single work ID
    if args.work_id:
        if args.kafka:
            fetchWorkDataAndSendToKafka(args.work_id, args.work_type, args.topic)
        else:
            if args.work_type == "novel":
                fetchNovelAndSaveToJson(args.work_id, args.output_dir)
            else:
                printInfo(f"Saving single illustration type not supported: {args.work_id}")
    
    # Process IDs from file
    elif args.ids_file:
        if args.kafka:
            processWorkIdsAndSendToKafka(args.ids_file, args.work_type, args.topic, args.threads)
        else:
            if args.work_type == "novel":
                processNovelIdsFromFile(args.ids_file, args.output_dir, args.threads)
            else:
                printInfo(f"Batch saving illustration type from file not supported")
    
    # Fetch from ranking
    elif args.ranking:
        crawler = RankingCrawler()
        ids = crawler.run()
        
        # If sending to Kafka
        if args.kafka and ids:
            # Ensure ids is list or set
            ids_list = list(ids) if isinstance(ids, set) else ids
            
            # Save ids to temp file
            temp_file = f"temp_{args.work_type}_ids.json"
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(ids_list, f)
            
            # Process and send to Kafka
            processWorkIdsAndSendToKafka(temp_file, args.work_type, args.topic, args.threads)
    
    else:
        print("""
Please specify one of the following parameters:
  --work-id ID      Process single work
  --ids-file FILE   Read ID list for batch processing
  --ranking         Fetch works from ranking

Optional parameters:
  --kafka           Send data to Kafka
  --topic TOPIC     Specify Kafka topic
  --work-type TYPE  Specify work type (novel or illust)
        """)