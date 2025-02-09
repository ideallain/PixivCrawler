import concurrent.futures as futures
import datetime
import re
import json
from typing import Set, Union

import tqdm
from pixiv_utils.pixiv_crawler.collector import Collector, collect, selectRanking, selectRankingPage
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

def fetchNovelAndSendToKafka(novel_id: str):
    """
    根据小说ID抓取阅读页 show.php?id=xxx, 
    直接提取 <meta name='preload-data' ...> 的 content (json_str) 并发送到 Kafka 的 'crawler-pixiv' 主题。
    """
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
        printInfo("[INFO] 未找到 <meta name='preload-data' id='meta-preload-data' ...> 标签.")
        return

    # 直接获取 meta 中的 content 属性（原始 JSON 字符串）
    json_str = meta_tag.get("content")
    if not json_str:
        printInfo("[INFO] meta-preload-data 标签中没有 content 或为空.")
        return

    # ========== (2) 直接发送原始 JSON 字符串到 Kafka ==========

    producer = getKafkaProducer()  # 获取(或创建)全局的 KafkaProducer
    future = producer.send(
        'crawler-pixiv',
        json_str.encode('utf-8')  # 将字符串手动转为 bytes
        # 可以考虑 key=novel_id.encode('utf-8') 等作为分区键
    )

    try:
        record_metadata = future.get(timeout=10)
        printInfo(
            f"[INFO] 已将 novel_id={novel_id} 的 meta-preload-data 发送到 Kafka "
            f"(topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset})"
        )
    except Exception as e:
        printInfo(f"[ERROR] 发送到 Kafka 出错: {e}")
class RankingCrawler:
    def __init__(self, capacity: float = 1024):
        """
        RankingCrawler download artworks from ranking

        Args:
            capacity (float, optional): The flow capacity in MB. Defaults to 1024.
        """
        self.date = ranking_config.start_date
        self.range = ranking_config.range
        self.mode = ranking_config.mode
        assert self.mode in ranking_config.ranking_modes, f"Invalid mode: {self.mode}"
        self.content = ranking_config.content_mode
        assert self.content in ranking_config.content_modes, f"Invalid content mode: {self.content}"

        # NOTE:
        #   1. url sample: "https://www.pixiv.net/ranking.php?mode=daily&content=all&date=20200801&p=1&format=json"
        #      url sample: "https://www.pixiv.net/ranking.php?mode=daily&content=illust&date=20220801&p=2&format=json"
        #   2. ref url sample: "https://www.pixiv.net/ranking.php?mode=daily&date=20200801"
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

            # 新起线程池，再次请求「小说详情页」生成 EPUB
            # printInfo(f"开始并行抓取 {len(all_image_ids)} 篇小说原文并生成 EPUB...")
            # with futures.ThreadPoolExecutor(download_config.num_threads) as executor:
            #     future_list2 = [
            #         executor.submit(fetchNovelAndGenerateEpub, novel_id)
            #         for novel_id in all_image_ids
            #     ]
            #     for _ in futures.as_completed(future_list2):
            #         pass  # 在这里可做进度更新或错误处理

            # 也可在这里并行调用 fetchNovelAndSendToKafka 进行发送 Kafka (视需求而定)
            printInfo(f"开始并行抓取 {len(all_image_ids)} 篇小说并发送至 Kafka...")
            with futures.ThreadPoolExecutor(download_config.num_threads) as executor:
                future_list3 = [
                    executor.submit(fetchNovelAndSendToKafka, novel_id)
                    for novel_id in all_image_ids
                ]
                for _ in futures.as_completed(future_list3):
                    pass 
        # 也可以把 all_image_ids 加入 collector
        self.collector.add(all_image_ids)

        printInfo(f"===== Collect {content} ranking complete =====")


    def run(self) -> Union[Set[str], float]:
        """
        Run the ranking crawler

        Returns:
            Union[Set[str], float]: artwork urls or download traffic usage
        """
        print("[DEBUG] RankingCrawler.run() is called")
        self._collect()
        print("[DEBUG] after self._collect()")
        # self.collector.collect()
        return self.downloader.download()
