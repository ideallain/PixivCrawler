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
# for test 
from kafka import KafkaProducer  # 来自 kafka-python
from bs4 import BeautifulSoup
from ebooklib import epub
import requests
from requests.models import Response


# ========== (1) 全局或单例的 KafkaProducer 缓存 ==========

_producer = None  # 用于缓存 KafkaProducer 的全局变量

def getKafkaProducer() -> KafkaProducer:
    """
    如果 _producer 为空则创建并缓存，否则直接返回已创建的 Producer。
    """
    global _producer
    if _producer is None:
        # 仅在第一次调用时实例化
        _producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],  # 修改为你的 Kafka broker 地址
            # 发送原始字节数组时不需要再 json.dumps()，这里的序列化可以只做 encode
            # 如果你想发送原始字符串，参考下行:
            # value_serializer=lambda v: v.encode('utf-8')  
        )
    return _producer

def metaPreloadDataToEpub(response: Response, epub_filename: str) -> None:
    """
    1. 在 HTML 中找到 <meta name="preload-data" id="meta-preload-data" content="...">
    2. 读取其 content 属性（JSON字符串）
    3. 解析 JSON (形如 {"novel": {"23933501": {...}}} )
    4. 在其中找到 content 字段（即小说正文）并生成 EPUB
    """
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # 1) 找到指定 meta 标签
    meta_tag = soup.find('meta', attrs={'name': 'preload-data', 'id': 'meta-preload-data'})
    if not meta_tag:
        print("[INFO] 未找到 <meta name='preload-data' id='meta-preload-data' ...> 标签.")
        return
    
    # 2) 读取其 content 属性（即 JSON 字符串）
    json_str = meta_tag.get("content")
    if not json_str:
        print("[INFO] meta-preload-data 标签中没有 content 属性，或属性值为空.")
        return

    # 3) 解析 JSON
    try:
        parsed_data = json.loads(json_str)
    except json.JSONDecodeError as e:
        print("[ERROR] meta-preload-data 中的内容不是合法 JSON:", e)
        return

    # 4) 获取 novel 字段，并取第一个 novel_id
    novel_data = parsed_data.get("novel")
    if not novel_data or not isinstance(novel_data, dict):
        print("[INFO] JSON 中未发现 'novel' 对象.")
        return

    # 如果只需要处理一个 id，这里演示取第一个 key
    # 若需要批量处理，可自行遍历 novel_data.keys()
    first_id = next(iter(novel_data))
    novel_info = novel_data.get(first_id, {})
    if not novel_info:
        print("[INFO] novel_data 中对应的小说信息为空.")
        return

    # 5) 拿到 content 字段的正文
    content_text = novel_info.get("content")
    if not content_text:
        print("[INFO] novel_info 中没有 'content' 字段.")
        return

    # 6) 生成 EPUB
    book = epub.EpubBook()
    book.set_identifier("novel-epub-id")
    book.set_language("zh")
    # 从 novel_info 中取 title/userName 以便更好地生成元信息
    title = novel_info.get("title", "无标题小说")
    author = novel_info.get("userName", "PixivCrawler")

    book.set_title(title)
    book.add_author(author)

    # 创建一个章节，章节内容即 content_text
    chapter = epub.EpubHtml(title="正文", file_name="chapter1.xhtml", lang="zh")
    chapter.content = content_text  # 如果是纯文本，可能需要转义或包裹 <p> 等标签

    book.add_item(chapter)

    # 设置目录与导航
    book.toc = (epub.Link('chapter1.xhtml', '正文', 'chapter1'),)
    book.add_item(epub.EpubNcx())
    book.add_item(epub.EpubNav())

    # 阅读顺序
    book.spine = ['nav', chapter]

    # 写入 EPUB 文件
    epub.write_epub(epub_filename, book, {})
    print(f"[INFO] 已生成 EPUB 文件: {epub_filename}")

def fetchWorkDataAndSendToKafka(work_id: str, work_type: str = "novel", topic: str = "pixiv_works"):
    """
    根据作品ID抓取数据并发送到Kafka
    
    Args:
        work_id (str): 作品ID
        work_type (str): 作品类型，默认为 "novel"，也可以是 "illust"
        topic (str): Kafka的主题名，默认为 "pixiv_works"
    """
    # 确定URL和请求头
    if work_type == "novel":
        url = f"https://www.pixiv.net/novel/show.php?id={work_id}"
    else:  # illust
        url = f"https://www.pixiv.net/artworks/{work_id}"
    
    headers = {
        "Referer": f"https://www.pixiv.net/{work_type}/",
        "x-requested-with": "XMLHttpRequest",
        "COOKIE": user_config.cookie or "",  # 使用配置中的cookie
    }

    try:
        # 发送请求获取数据
        printInfo(f"[INFO] 正在抓取作品 {work_id} 的数据...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except Exception as e:
        printInfo(f"[ERROR] 抓取作品({work_id})出错: {e}")
        return False

    # 解析页面，获取元数据
    soup = BeautifulSoup(response.text, 'html.parser')
    meta_tag = soup.find('meta', attrs={'name': 'preload-data', 'id': 'meta-preload-data'})
    
    if not meta_tag:
        printInfo(f"[INFO] 未找到作品 {work_id} 的 meta-preload-data 标签")
        return False

    # 获取JSON字符串
    json_str = meta_tag.get("content")
    if not json_str:
        printInfo(f"[INFO] 作品 {work_id} 的 meta-preload-data 标签中没有 content 或为空")
        return False

    try:
        # 解析JSON数据
        parsed_data = json.loads(json_str)
        
        # 添加一些额外信息
        message_data = {
            "work_id": work_id,
            "work_type": work_type,
            "fetch_time": datetime.datetime.now().isoformat(),
            "data": parsed_data
        }
        
        # 获取Kafka生产者实例
        producer = getKafkaProducer()
        
        # 将数据序列化为JSON并发送到Kafka
        json_data = json.dumps(message_data, ensure_ascii=False).encode('utf-8')
        producer.send(topic, json_data)
        print(json_data)
        
        # 确保消息发送成功
        producer.flush()
        
        printInfo(f"[INFO] 已将作品 {work_id} 的数据发送到Kafka主题 {topic}")
        return True
    except json.JSONDecodeError as e:
        printInfo(f"[ERROR] 作品 {work_id} 的数据不是合法JSON: {e}")
    except Exception as e:
        printInfo(f"[ERROR] 发送作品 {work_id} 的数据到Kafka时出错: {e}")
    
    return False


def processWorkIdsAndSendToKafka(ids_file: str, work_type: str = "novel", topic: str = "pixiv_works", num_threads: int = None):
    """
    从指定的JSON文件中读取作品ID列表，并并行抓取每个作品的数据发送到Kafka
    
    Args:
        ids_file (str): 包含作品ID列表的JSON文件路径
        work_type (str): 作品类型，默认为 "novel"，也可以是 "illust"
        topic (str): Kafka的主题名，默认为 "pixiv_works"
        num_threads (int): 并行线程数，默认使用配置中的线程数
    """
    # 使用配置中的线程数或指定的线程数
    threads = num_threads if num_threads is not None else download_config.num_threads
    
    try:
        # 读取作品ID列表
        with open(ids_file, "r", encoding="utf-8") as f:
            work_ids = json.load(f)
        
        if not isinstance(work_ids, list):
            printInfo(f"[ERROR] {ids_file} 的内容不是有效的ID列表")
            return
        
        printInfo(f"[INFO] 从 {ids_file} 中读取到 {len(work_ids)} 个作品ID")
        
        # 并行抓取每个作品的数据并发送到Kafka
        printInfo(f"[INFO] 开始并行抓取 {len(work_ids)} 个作品数据并发送到Kafka，使用 {threads} 个线程...")
        
        success_count = 0
        fail_count = 0
        
        with futures.ThreadPoolExecutor(threads) as executor:
            with tqdm.tqdm(total=len(work_ids), desc=f"发送{work_type}到Kafka") as pbar:
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
        
        printInfo(f"[INFO] 所有作品数据处理完成: 成功 {success_count}, 失败 {fail_count}")
        
    except Exception as e:
        printInfo(f"[ERROR] 处理作品ID列表时出错: {e}")

def fetchNovelAndGenerateEpub(novel_id: str):
    """
    根据小说 ID 构造阅读页URL, 请求后调用 selectCharcoalTokenAndGenerateEpub 生成 EPUB。
    注意：如果有自定义Header或Cookie，需要在此处理。
    """
    # Pixiv 的小说阅读 URL
    url = f"https://www.pixiv.net/novel/show.php?id={novel_id}"

    headers = {
        "Referer": "https://www.pixiv.net/novel/",
        "x-requested-with": "XMLHttpRequest",
        "COOKIE": user_config.cookie,  # 如果需要 Pixiv 登录态
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        # 生成 epub 文件名，可根据小说ID来命名
        epub_filename = f"novel_{novel_id}.epub"
        metaPreloadDataToEpub(response, epub_filename)
    except Exception as e:
        printInfo(f"抓取小说 {novel_id} 出错: {e}")

def fetchNovelAndSaveToJson(novel_id: str, output_dir: str = "novels_json"):
    """
    根据小说ID抓取阅读页 show.php?id=xxx, 
    直接提取 <meta name='preload-data' ...> 的 content (json_str) 并保存到JSON文件。
    
    Args:
        novel_id (str): 小说ID
        output_dir (str): 输出目录，默认为 "novels_json"
    """
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 构造输出文件路径
    output_file = os.path.join(output_dir, f"novel_{novel_id}.json")
    
    # 如果文件已存在，跳过抓取
    if os.path.exists(output_file):
        printInfo(f"[INFO] 小说 {novel_id} 的JSON文件已存在，跳过抓取")
        return
    
    # Pixiv 的小说阅读 URL
    url = f"https://www.pixiv.net/novel/show.php?id={novel_id}"
    headers = {
        "Referer": "https://www.pixiv.net/novel/",
        "x-requested-with": "XMLHttpRequest",
        "COOKIE": user_config.cookie or "",  # 如果需要 Pixiv 登录态
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except Exception as e:
        printInfo(f"[WARN] 抓取小说页面({novel_id})出错: {e}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    meta_tag = soup.find('meta', attrs={'name': 'preload-data', 'id': 'meta-preload-data'})
    if not meta_tag:
        printInfo(f"[INFO] 未找到小说 {novel_id} 的 meta-preload-data 标签")
        return

    # 获取 meta 中的 content 属性（原始 JSON 字符串）
    json_str = meta_tag.get("content")
    if not json_str:
        printInfo(f"[INFO] 小说 {novel_id} 的 meta-preload-data 标签中没有 content 或为空")
        return

    try:
        # 解析 JSON 以确保格式正确
        parsed_data = json.loads(json_str)
        
        # 将解析后的数据保存到文件
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(parsed_data, f, indent=4, ensure_ascii=False)
        
        printInfo(f"[INFO] 已将小说 {novel_id} 的数据保存到 {output_file}")
    except json.JSONDecodeError as e:
        printInfo(f"[ERROR] 小说 {novel_id} 的数据不是合法 JSON: {e}")
    except Exception as e:
        printInfo(f"[ERROR] 保存小说 {novel_id} 的数据时出错: {e}")

def processNovelIdsFromFile(ids_file: str, output_dir: str = "novels_json", num_threads: int = None):
    """
    从指定的JSON文件中读取小说ID列表，并并行抓取每个小说的数据保存为独立的JSON文件
    
    Args:
        ids_file (str): 包含小说ID列表的JSON文件路径
        output_dir (str): 输出目录，默认为 "novels_json"
        num_threads (int): 并行线程数，默认使用配置中的线程数
    """
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 使用配置中的线程数或指定的线程数
    threads = num_threads if num_threads is not None else download_config.num_threads
    
    try:
        # 读取小说ID列表
        with open(ids_file, "r", encoding="utf-8") as f:
            novel_ids = json.load(f)
        
        if not isinstance(novel_ids, list):
            printInfo(f"[ERROR] {ids_file} 的内容不是有效的ID列表")
            return
        
        printInfo(f"[INFO] 从 {ids_file} 中读取到 {len(novel_ids)} 个小说ID")
        
        # 并行抓取每个小说的数据
        printInfo(f"[INFO] 开始并行抓取 {len(novel_ids)} 篇小说数据，使用 {threads} 个线程...")
        
        with futures.ThreadPoolExecutor(threads) as executor:
            with tqdm.tqdm(total=len(novel_ids), desc="抓取小说数据") as pbar:
                future_list = [
                    executor.submit(fetchNovelAndSaveToJson, novel_id, output_dir)
                    for novel_id in novel_ids
                ]
                
                for future in futures.as_completed(future_list):
                    # 更新进度条
                    pbar.update(1)
        
        printInfo(f"[INFO] 所有小说数据抓取完成，已保存到 {output_dir} 目录")
        
    except Exception as e:
        printInfo(f"[ERROR] 处理小说ID列表时出错: {e}")

class RankingCrawler:
    def __init__(self, capacity: float = 1024, recommends_tags: List[str] = None, max_pages = 5):
        """
        RankingCrawler download artworks from ranking

        Args:
            capacity (float, optional): The flow capacity in MB. Defaults to 1024.
        """
        self.date = ranking_config.start_date
        self.range = ranking_config.range
        self.mode = ranking_config.mode

        self.is_recommends_mode = recommends_tags is not None
        # config only for recommends mode
        if self.is_recommends_mode:
            self.recommends_tags = recommends_tags
            self.max_pages = max_pages

        # NOTE:
        #   1. url sample: "https://www.pixiv.net/ranking.php?mode=daily&content=all&date=20200801&p=1&format=json"
        #      url sample: "https://www.pixiv.net/ranking.php?mode=daily&content=illust&date=20220801&p=2&format=json"
        #   2. ref url sample: "https://www.pixiv.net/ranking.php?mode=daily&date=20200801"
        if not self.is_recommends_mode:
            self.content = ranking_config.content_mode
            assert self.mode in ranking_config.ranking_modes, f"Invalid mode: {self.mode}"
            assert self.content in ranking_config.content_modes, f"Invalid content mode: {self.content}"

            if self.content == "novel": 
                base_url = "https://www.pixiv.net/novel/ranking.php?"
            else:
                base_url = "https://www.pixiv.net/ranking.php?"
            self.url_template =  base_url + "&".join(
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
        if self.is_recommends_mode:
            self._collect_recommends()
        else: 
            self._collect_ranking(artworks_per_json)

    def _collect_recommends(self):
        """
        Collect novel data from recommendation pages for specified tags
        """
        printInfo(f"===== Start collecting recommendations for {len(self.recommends_tags)} tags =====")
        
        # 存储所有小说数据
        all_novel_data = []
        all_novel_ids = set()
        
        # 构造所有请求 URL
        urls = set()
        additional_headers = []
        
        # 每个标签默认获取前5页（可以根据需要调整）
        
        for tag in self.recommends_tags:
            encoded_tag = requests.utils.quote(tag)
            for page in range(1, self.max_pages + 1):
                url = f"https://www.pixiv.net/ajax/search/novels/{encoded_tag}?word={encoded_tag}&order=date_d&mode=all&p={page}&csw=0&s_mode=s_tag&gs=1&lang=zh_tw&version=e11a103b148c138660cbd4085334f4e6da87a9f1"
                urls.add(url)
                
                # 为每个URL添加对应header
                referer_value = f"https://www.pixiv.net/tags/{encoded_tag}/novels"
                additional_headers.append({
                    "Referer": referer_value,
                    "x-requested-with": "XMLHttpRequest",
                    "COOKIE": user_config.cookie,
                })
        
        printInfo(f"{download_config.num_threads} threads are used to collect recommendations")
        
        with futures.ThreadPoolExecutor(download_config.num_threads) as executor:
            with tqdm.tqdm(total=len(urls), desc="Collecting recommendations") as pbar:
                # 提交所有任务
                futures_list = [
                    executor.submit(collect, url, selectRecommends, header)
                    for url, header in zip(urls, additional_headers)
                ]
                
                # 收集所有 Future 的结果
                for future in futures.as_completed(futures_list):
                    body_data = future.result()
                    if body_data is not None and "novel" in body_data and "data" in body_data["novel"]:
                        novels_data = body_data["novel"]["data"]
                        
                        # 添加到总数据集
                        all_novel_data.extend(novels_data)
                        
                        # 提取小说ID
                        for novel in novels_data:
                            if "novelId" in novel:
                                all_novel_ids.add(novel["novelId"])
                            elif "latestEpisodeId" in novel:
                                all_novel_ids.add(novel["latestEpisodeId"])
                    
                    pbar.update(1)
        
        # 保存完整数据到JSON文件
        with open("novels_from_recommends.json", "w", encoding="utf-8") as f:
            json.dump(all_novel_data, f, indent=4, ensure_ascii=False)
        
        # 保存ID列表（如果需要单独的ID列表）
        self._save_to_json(all_novel_ids, "novel_ids_from_recommends.json")
        
        printInfo(f"Collected {len(all_novel_data)} novel entries with {len(all_novel_ids)} unique IDs")
        
        # 添加到collector以供后续处理
        self.collector.add(all_novel_ids)
        
        printInfo(f"===== Collection complete =====")

    # def _collect_recommends(self):
    #     """
    #     Collect novel and series IDs from recommendation pages for specified tags
    #     """
    #     printInfo(f"===== Start collecting recommendations for {len(self.recommends_tags)} tags =====")
        
    #     # Will store all collected IDs here
    #     all_novel_ids = set()
    #     all_series_ids = set()
        
    #     # 构造所有请求 URL
    #     urls = set()
    #     additional_headers = []
        
    #     # 每个标签默认获取前10页（可以根据需要调整）
    #     max_pages = 3
        
    #     for tag in self.recommends_tags:
    #         encoded_tag = requests.utils.quote(tag)
    #         for page in range(1, max_pages + 1):
    #             url = f"https://www.pixiv.net/ajax/search/novels/{encoded_tag}?word={encoded_tag}&order=date_d&mode=all&p={page}&csw=0&s_mode=s_tag_full&gs=1&lang=zh_tw&version=e11a103b148c138660cbd4085334f4e6da87a9f1"
    #             urls.add(url)
                
    #             # 为每个URL添加对应header
    #             referer_value = f"https://www.pixiv.net/tags/{encoded_tag}/novels"
    #             additional_headers.append({
    #                 "Referer": referer_value,
    #                 "x-requested-with": "XMLHttpRequest",
    #                 "COOKIE": user_config.cookie,
    #             })
        
    #     printInfo(f"{download_config.num_threads} threads are used to collect recommendations")
        
    #     with futures.ThreadPoolExecutor(download_config.num_threads) as executor:
    #         with tqdm.tqdm(total=len(urls), desc="Collecting recommendations") as pbar:
    #             # 提交所有任务
    #             futures_list = [
    #                 executor.submit(collect, url, selectRecommends, header)
    #                 for url, header in zip(urls, additional_headers)
    #             ]
                
    #             # 收集所有 Future 的结果
    #             for future in futures.as_completed(futures_list):
    #                 result = future.result()
    #                 if result is not None:
    #                     # 解包结果
    #                     if isinstance(result, tuple) and len(result) == 3:
    #                         novel_ids, series_ids, _ = result
    #                     else:
    #                         # 如果结果不符合预期，假设只返回了novel_ids
    #                         novel_ids, series_ids = result, set()
                        
    #                     # 更新收集的ID集合
    #                     all_novel_ids.update(novel_ids)
    #                     all_series_ids.update(series_ids)
                        
    #                 pbar.update(1)
        
    #     # 保存结果到JSON文件
    #     self._save_to_json(all_novel_ids, "novel_ids_from_recommends.json")
    #     if all_series_ids:
    #         self._save_to_json(all_series_ids, "series_ids_from_recommends.json")
        
    #     printInfo(f"Collected {len(all_novel_ids)} novel IDs and {len(all_series_ids)} series IDs")
        
    #     # 添加到collector以供后续处理
    #     self.collector.add(all_novel_ids)
        
    #     printInfo(f"===== Collection complete =====")

    # def _collect_recommends(self):
    #     """
    #     Collect novel and series IDs from recommendation pages for specified tags
    #     """
    #     printInfo(f"===== Start collecting recommendations for {len(self.recommends_tags)} tags =====")
        
    #     # Will store all collected IDs here
    #     all_novel_ids = set()
    #     all_series_ids = set()
        
    #     with futures.ThreadPoolExecutor(download_config.num_threads) as executor:
    #         future_list = []
            
    #         # Submit all tag pages to be processed
    #         for tag in self.recommends_tags:
    #             printInfo(f"Processing tag: {tag}")
    #             # Start with page 1
    #             future_list.append(executor.submit(
    #                 self._process_tag_page, tag, 1
    #             ))
            
    #         # Process results as they complete
    #         with tqdm.tqdm(total=len(future_list), desc="Collecting tag pages") as pbar:
    #             for future in futures.as_completed(future_list):
    #                 result = future.result()
    #                 if result:
    #                     novel_ids, series_ids, next_pages = result
    #                     all_novel_ids.update(novel_ids)
    #                     all_series_ids.update(series_ids)
                        
    #                     # Add additional page requests for this tag
    #                     for page in next_pages:
    #                         future_list.append(executor.submit(
    #                             self._process_tag_page, tag, page
    #                         ))
    #                         pbar.total += 1
                    
    #                 pbar.update(1)
        
    #     # Save all collected IDs to JSON files
    #     self._save_to_json(all_novel_ids, "novel_ids_from_recommends.json")
    #     self._save_to_json(all_series_ids, "series_ids_from_recommends.json")
        
    #     printInfo(f"Collected {len(all_novel_ids)} novel IDs and {len(all_series_ids)} series IDs")
        
    #     # Add novel IDs to collector for processing
    #     self.collector.add(all_novel_ids)
        
    #     printInfo(f"===== Collection complete =====")

    def _process_tag_page(self, tag: str, page: int):
        """
        Process a single tag page and extract novel and series IDs
        
        Args:
            tag (str): Tag name
            page (int): Page number
            
        Returns:
            tuple: (novel_ids, series_ids, next_pages)
        """
        # url = f"https://www.pixiv.net/tags/{tag}/novels?gs=1&p={page}"
        # tag should be encoded
        encoded_tag = requests.utils.quote(tag)
        url = f"https://www.pixiv.net/ajax/search/novels/{encoded_tag}?word={encoded_tag}&order=date_d&mode=all&p={page}&csw=0&s_mode=s_tag_full&gs=1&lang=zh_tw&version=e11a103b148c138660cbd4085334f4e6da87a9f1"
        headers = {
            "Referer": "https://www.pixiv.net/",
            "x-requested-with": "XMLHttpRequest",
            "COOKIE": user_config.cookie or "",
        }
        
        try:
            # Add a small random delay for rate limiting
            time.sleep(random.uniform(0.5, 2.0))
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            # Extract IDs using the selector function
            novel_ids, series_ids, has_next = selectRecommends(response)
            
            # Determine if there are more pages to fetch
            next_pages = []
            if has_next and page < 10:  # Limit to max 10 pages per tag
                next_pages.append(page + 1)
                
            return novel_ids, series_ids, next_pages
            
        except Exception as e:
            printInfo(f"Error processing tag page {tag}, page {page}: {e}")
            return set(), set(), []

    def _save_to_json(self, ids: Set[str], filename: str):
        """
        Save set of IDs to a JSON file
        
        Args:
            ids (Set[str]): Set of IDs to save
            filename (str): Name of the JSON file
        """
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(list(ids), f, indent=4, ensure_ascii=False)
        printInfo(f"Saved {len(ids)} IDs to {filename}")

    def _collect_ranking(self, artworks_per_json: int = 50):
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

        # 构造所有请求 URL
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
                    # 用正则从 url 截取 Referer
                    referer_match = re.search("(.*)&p", url)
                    referer_value = referer_match.group(1) if referer_match else url
                    additional_headers.append({
                        "Referer": referer_value,
                        "x-requested-with": "XMLHttpRequest",
                        "COOKIE": user_config.cookie,
                    })

                # 根据是否 novel，决定调用哪个 selector
                if self.content == "novel":
                    printInfo(f"Collecting novel ranking page...")
                    selector_func = selectRankingPage
                else:
                    printInfo(f"Collecting illustration ranking page...")
                    selector_func = selectRanking

                # 提交所有任务
                futures_list = [
                    executor.submit(collect, url, selector_func, header)
                    for url, header in zip(urls, additional_headers)
                ]

                # 收集所有 Future 的结果
                all_image_ids = set()
                for future in futures.as_completed(futures_list):
                    image_ids = future.result()
                    if image_ids is not None:
                        # 这里先把本次解析到的ID加到总集合中
                        all_image_ids.update(image_ids)
                        printInfo(f"{all_image_ids}")
                    pbar.update()

        # 统一把 all_image_ids 写到 JSON 文件（只做一次）
        if self.content == "novel":
            file_path = "novel_image_ids.json"
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(list(all_image_ids), f, indent=4, ensure_ascii=False)
            printInfo(f"全部小说 ID 已保存到 {file_path}")

            # 使用新的函数保存小说数据到JSON文件
            output_dir = "novels_json"
            printInfo(f"开始并行抓取 {len(all_image_ids)} 篇小说并保存为JSON文件...")
            
            os.makedirs(output_dir, exist_ok=True)
            with futures.ThreadPoolExecutor(download_config.num_threads) as executor:
                future_list3 = [
                    executor.submit(fetchNovelAndSaveToJson, novel_id, output_dir)
                    for novel_id in all_image_ids
                ]
                with tqdm.tqdm(total=len(all_image_ids), desc="抓取小说JSON") as pbar:
                    for _ in futures.as_completed(future_list3):
                        pbar.update(1)
            
            printInfo(f"所有小说数据已保存到 {output_dir} 目录")
            
        # 也可以把 all_image_ids 加入 collector
        self.collector.add(all_image_ids)

        printInfo(f"===== Collect {content} ranking complete =====")


    def run(self) -> Union[Set[str], float]:
        """
        Run the ranking | recommend crawler

        Returns:
            Union[Set[str], float]: artwork urls or download traffic usage
        """
        print("[DEBUG] RankingCrawler.run() is called")
        self._collect()
        print("[DEBUG] after self._collect()")
        # self.collector.collect()
        return self.downloader.download()

# 命令行入口点，支持从文件读取ID列表并处理
# if __name__ == "__main__":
#     import argparse
    
#     parser = argparse.ArgumentParser(description="Pixiv Novel Crawler")
#     parser.add_argument("--ids-file", help="从指定的JSON文件中读取小说ID列表")
#     parser.add_argument("--output-dir", default="novels_json", help="输出目录，默认为 novels_json")
#     parser.add_argument("--threads", type=int, help="并行线程数，默认使用配置中的线程数")
#     parser.add_argument("--ranking", action="store_true", help="从排行榜抓取小说")
    
#     args = parser.parse_args()
    
#     if args.ids_file:
#         processNovelIdsFromFile(args.ids_file, args.output_dir, args.threads)
#     elif args.ranking:
#         # 从排行榜抓取
#         crawler = RankingCrawler()
#         crawler.run()
#     else:
#         print("请指定 --ids-file 参数读取ID列表或使用 --ranking 从排行榜抓取")


# 命令行入口点，增加Kafka功能支持
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Pixiv Crawler with Kafka Support")
    parser.add_argument("--ids-file", help="从指定的JSON文件中读取作品ID列表")
    parser.add_argument("--output-dir", default="novels_json", help="输出目录，默认为 novels_json")
    parser.add_argument("--threads", type=int, help="并行线程数，默认使用配置中的线程数")
    parser.add_argument("--ranking", action="store_true", help="从排行榜抓取作品")
    parser.add_argument("--work-type", default="novel", choices=["novel", "illust"], 
                        help="作品类型：novel 或 illust，默认为 novel")
    parser.add_argument("--kafka", action="store_true", help="是否发送数据到Kafka")
    parser.add_argument("--topic", default="pixiv_works", help="Kafka主题名，默认为 pixiv_works")
    parser.add_argument("--work-id", help="单个作品ID，直接抓取并处理")
    
    args = parser.parse_args()
    
    # 处理单个作品ID
    if args.work_id:
        if args.kafka:
            fetchWorkDataAndSendToKafka(args.work_id, args.work_type, args.topic)
        else:
            if args.work_type == "novel":
                fetchNovelAndSaveToJson(args.work_id, args.output_dir)
            else:
                printInfo(f"暂不支持保存单个插画类型: {args.work_id}")
    
    # 从文件读取ID列表并处理
    elif args.ids_file:
        if args.kafka:
            processWorkIdsAndSendToKafka(args.ids_file, args.work_type, args.topic, args.threads)
        else:
            if args.work_type == "novel":
                processNovelIdsFromFile(args.ids_file, args.output_dir, args.threads)
            else:
                printInfo(f"暂不支持从文件批量保存插画类型")
    
    # 从排行榜抓取
    elif args.ranking:
        crawler = RankingCrawler()
        ids = crawler.run()
        
        # 如果需要发送到Kafka
        if args.kafka and ids:
            # 确保ids是集合或列表
            ids_list = list(ids) if isinstance(ids, set) else ids
            
            # 将ids保存到临时文件
            temp_file = f"temp_{args.work_type}_ids.json"
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(ids_list, f)
            
            # 处理并发送到Kafka
            processWorkIdsAndSendToKafka(temp_file, args.work_type, args.topic, args.threads)
    
    else:
        print("""
请指定以下参数之一:
  --work-id ID      处理单个作品
  --ids-file FILE   读取ID列表进行批量处理
  --ranking         从排行榜抓取作品

可选参数:
  --kafka           发送数据到Kafka
  --topic TOPIC     指定Kafka主题
  --work-type TYPE  指定作品类型 (novel 或 illust)
        """)