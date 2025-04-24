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

## for test
def playGetNovelFromIdAndSendToKafka(work_id: str, work_type: str = "novel", topic: str = "pixiv_works"):
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
        # print(json_data)
        
        # Ensure message is sent
        producer.flush()
        
        printInfo(f"[INFO] Data for work {work_id} sent to Kafka topic {topic}")
        return True
    except json.JSONDecodeError as e:
        printInfo(f"[ERROR] Data for work {work_id} is not valid JSON: {e}")
    except Exception as e:
        printInfo(f"[ERROR] Error sending data for work {work_id} to Kafka: {e}")
    
    return False


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
# pixiv_novels 是 我们现在关心的 pixiv_works是历史了
def sendNovelToKafka(novel_id: str, parsed_data: dict, topic: str = "pixiv_novels"):
    """
    将Pixiv小说数据转换为结构化格式并发送到Kafka
    
    Args:
        novel_id: 小说ID
        parsed_data: 解析后的小说JSON数据
        topic: Kafka主题名称
    
    Returns:
        bool: 是否成功发送
    """
    try:
        # 提取小说数据 - 处理不同的JSON结构可能性
        novel_data = None
        if "novel" in parsed_data and novel_id in parsed_data["novel"]:
            novel_data = parsed_data["novel"][novel_id]
        else:
            # 尝试在嵌套结构中查找小说ID
            for key, value in parsed_data.items():
                if isinstance(value, dict) and "novel" in value and novel_id in value["novel"]:
                    novel_data = value["novel"][novel_id]
                    break
        
        if not novel_data:
            printInfo(f"[ERROR] Could not find novel data for ID {novel_id} in parsed data")
            return False
        
        # 提取元数据
        title = novel_data.get("title", "")
        author_id = novel_data.get("userId", "")
        author_name = novel_data.get("userName", "")
        created_at = novel_data.get("createDate", "")
        updated_at = novel_data.get("updateDate", "")
        description = novel_data.get("description", "")
        content = novel_data.get("content", "")
        text_length = novel_data.get("textLength", 0)
        language = novel_data.get("language", "")
        
        # 提取标签
        tags = []
        if "tags" in novel_data and "tags" in novel_data["tags"]:
            tags = novel_data["tags"]["tags"]
        
        # 提取评分
        bookmark_count = novel_data.get("bookmarkCount", 0)
        view_count = novel_data.get("viewCount", 0)
        like_count = novel_data.get("likeCount", 0)
        
        # 构建Kafka消息
        kafka_message = {
            "id": novel_id,
            "type": "novel",
            "metadata": {
                "title": title,
                "author": {
                    "id": author_id,
                    "name": author_name
                },
                "created_at": created_at,
                "updated_at": updated_at,
                "tags": tags,
                "description": description,
                "text_length": text_length,
                "language": language,
                "bookmark_count": bookmark_count,
                "rating": {
                    "view_count": view_count,
                    "like_count": like_count
                }
            },
            "content": content,
            "status": {
                "llm_processed": False,
                "llm_task_id": None,
                "llm_tags": [],
                "last_processed": datetime.datetime.now().isoformat()
            },
            "raw_data": parsed_data  # 包含完整原始数据以供参考
        }
        
        # 获取Kafka生产者
        producer = getKafkaProducer()
        
        # 序列化并发送到Kafka
        json_data = json.dumps(kafka_message, ensure_ascii=False).encode('utf-8')
        producer.send(topic, json_data)
        
        # 确保消息已发送
        producer.flush()
        
        printInfo(f"[INFO] Novel {novel_id} data sent to Kafka topic {topic}")
        return True
        
    except Exception as e:
        printInfo(f"[ERROR] Error sending novel {novel_id} to Kafka: {e}")
        return False

def fetchNovelAndSaveToJson(novel_id: str, output_dir: str = "novels_json", send_to_kafka: bool = False, topic: str = "pixiv_novels"):
    """
    获取小说数据，保存为JSON并可选择发送到Kafka
    
    Args:
        novel_id: 小说ID
        output_dir: 输出目录路径
        send_to_kafka: 是否发送到Kafka
        topic: Kafka主题名称
    
    Returns:
        bool: 是否成功获取数据
    """
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 构建输出文件路径
    output_file = os.path.join(output_dir, f"novel_{novel_id}.json")
    
    # 检查文件是否已存在
    file_exists = os.path.exists(output_file)
    
    # 如果文件已存在且不需要发送到Kafka，则跳过
    if file_exists and not send_to_kafka:
        printInfo(f"[INFO] JSON file for novel {novel_id} already exists, skipping")
        return True
    
    # 如果文件存在且需要发送到Kafka，可以使用现有数据
    if file_exists and send_to_kafka:
        try:
            with open(output_file, "r", encoding="utf-8") as f:
                parsed_data = json.load(f)
                return sendNovelToKafka(novel_id, parsed_data, topic)
        except Exception as e:
            printInfo(f"[WARN] Error reading existing novel data ({novel_id}): {e}, will refetch")
    
    # Pixiv小说阅读URL
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
        return False

    soup = BeautifulSoup(response.text, 'html.parser')
    meta_tag = soup.find('meta', attrs={'name': 'preload-data', 'id': 'meta-preload-data'})
    if not meta_tag:
        printInfo(f"[INFO] No meta-preload-data tag found for novel {novel_id}")
        return False

    # 获取content属性（原始JSON字符串）
    json_str = meta_tag.get("content")
    if not json_str:
        printInfo(f"[INFO] meta-preload-data tag has no content or is empty for novel {novel_id}")
        return False

    try:
        # 解析JSON以确保格式正确
        parsed_data = json.loads(json_str)
        
        # 保存解析后的数据到文件
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(parsed_data, f, indent=4, ensure_ascii=False)
        
        printInfo(f"[INFO] Data for novel {novel_id} saved to {output_file}")
        
        # 如果需要，发送到Kafka
        if send_to_kafka:
            return sendNovelToKafka(novel_id, parsed_data, topic)
        
        return True
            
    except json.JSONDecodeError as e:
        printInfo(f"[ERROR] Data for novel {novel_id} is not valid JSON: {e}")
    except Exception as e:
        printInfo(f"[ERROR] Error saving data for novel {novel_id}: {e}")
    
    return False

def processNovelIdsFromFile(ids_file: str, output_dir: str = "novels_json", num_threads: int = None, 
                           send_to_kafka: bool = False, topic: str = "pixiv_novels"):
    """
    从JSON文件读取小说ID列表，保存每个小说的数据为独立的JSON文件
    可选择发送到Kafka
    
    Args:
        ids_file: 包含小说ID列表的JSON文件路径
        output_dir: 输出目录路径
        num_threads: 线程数，如果为None则使用配置值
        send_to_kafka: 是否发送到Kafka
        topic: Kafka主题名称
    """
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 使用配置的线程数或指定的线程数
    threads = num_threads if num_threads is not None else download_config.num_threads
    
    try:
        # 读取小说ID列表
        with open(ids_file, "r", encoding="utf-8") as f:
            novel_ids = json.load(f)
        
        if not isinstance(novel_ids, list):
            printInfo(f"[ERROR] Content of {ids_file} is not a valid ID list")
            return
        
        printInfo(f"[INFO] Read {len(novel_ids)} novel IDs from {ids_file}")
        
        # 并行处理
        action_msg = "fetching and sending to Kafka" if send_to_kafka else "fetching"
        printInfo(f"[INFO] Starting parallel processing of {len(novel_ids)} novels ({action_msg}) using {threads} threads...")
        
        success_count = 0
        fail_count = 0
        
        with futures.ThreadPoolExecutor(threads) as executor:
            with tqdm.tqdm(total=len(novel_ids), desc="Fetching novel data") as pbar:
                future_list = [
                    executor.submit(fetchNovelAndSaveToJson, novel_id, output_dir, send_to_kafka, topic)
                    for novel_id in novel_ids
                ]
                
                for future in futures.as_completed(future_list):
                    result = future.result()
                    if result:
                        success_count += 1
                    else:
                        fail_count += 1
                    pbar.update(1)
        
        printInfo(f"[INFO] All novel data {action_msg} complete: {success_count} successful, {fail_count} failed")
        printInfo(f"[INFO] Data saved to {output_dir} directory")
        
    except Exception as e:
        printInfo(f"[ERROR] Error processing novel ID list: {e}")

    
class RankingCrawler:
    def __init__(self, capacity: float = 1024, recommends_tags: List[str] = None, max_pages = 5, 
                send_to_kafka: bool = False, kafka_topic: str = "pixiv_novels"):
        """
        Initialize RankingCrawler to download artworks from ranking or recommendations
        
        Args:
            capacity (float): Flow capacity in MB, default 1024
            recommends_tags (List[str]): Tags for recommendations mode
            max_pages (int): Maximum pages to fetch per tag, default 5
            send_to_kafka (bool): Whether to send data to Kafka
            kafka_topic (str): Kafka topic name
        """
        self.date = ranking_config.start_date
        self.range = ranking_config.range
        self.mode = ranking_config.mode
        
        # Kafka settings
        self.send_to_kafka = send_to_kafka
        self.kafka_topic = kafka_topic

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

    def collect_recommends(self):
        """Collect novel data from IDs in a text file and process them"""
        printInfo("===== Start processing novels from ID file =====")
        
        # Read novel IDs from text file
        all_novel_ids = set()
        try:
            with open("ids.txt", "r", encoding="utf-8") as f:
                for line in f:
                    novel_id = line.strip()
                    if novel_id:  # Skip empty lines
                        all_novel_ids.add(novel_id)
        except FileNotFoundError:
            printInfo("Error: ids.txt file not found!")
            return
        except Exception as e:
            printInfo(f"Error reading IDs file: {e}")
            return
        
        printInfo(f"Loaded {len(all_novel_ids)} novel IDs from ids.txt")
        
        # Process the IDs and send to Kafka
        if self.send_to_kafka:
            output_dir = os.path.join(download_config.store_path, "novels_recommend_json") 
            printInfo(f"{download_config.num_threads} threads are used to process novels")
            
            with futures.ThreadPoolExecutor(download_config.num_threads) as executor:
                future_list2 = [
                    executor.submit(fetchNovelAndSaveToJson, novel_id, output_dir, self.send_to_kafka, self.kafka_topic)
                    for novel_id in all_novel_ids
                ]
                with tqdm.tqdm(total=len(all_novel_ids), desc="Fetching novel JSON") as pbar:
                    for _ in futures.as_completed(future_list2):
                        pbar.update(1)
        
        # Save ID list
        self._save_to_json(all_novel_ids, "novel_ids_from_file.json")
        
        printInfo(f"Processed {len(all_novel_ids)} unique novel IDs")
        
        # Add to collector for further processing
        self.collector.add(all_novel_ids)
        
        printInfo(f"===== Processing complete =====")

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
                        # printInfo(f"{all_image_ids}")
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
                    executor.submit(fetchNovelAndSaveToJson, novel_id, output_dir, self.send_to_kafka, self.kafka_topic)
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


