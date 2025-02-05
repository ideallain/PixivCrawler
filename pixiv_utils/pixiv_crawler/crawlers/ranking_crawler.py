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
        
        # 也可以把 all_image_ids 加入 collector
        self.collector.add(all_image_ids)

        printInfo(f"===== Collect {content} ranking complete =====")

    # def _collect(self, artworks_per_json: int = 50):
    #     """
    #     Collect illust_id from ranking

    #     Args:
    #         artworks_per_json: Number of artworks per ranking.json. Defaults to 50.
    #     """
    #     num_page = (ranking_config.num_artwork - 1) // artworks_per_json + 1  # ceil
    #     printInfo(f"Collecting {self.content} ranking from {num_page} pages")

    #     def addDate(current: datetime.date, days):
    #         return current + datetime.timedelta(days)

    #     content = f"{self.mode}:{self.content}"
    #     printInfo(f"===== Start collecting {content} ranking =====")
    #     printInfo(
    #         "From {} to {}".format(
    #             self.date.strftime("%Y-%m-%d"),
    #             addDate(self.date, self.range - 1).strftime("%Y-%m-%d"),
    #         )
    #     )

    #     urls: Set[str] = set()
    #     for _ in range(self.range):
    #         for i in range(num_page):
    #             urls.add(self.url_template.format(self.date.strftime("%Y%m%d"), i + 1))
    #         self.date = addDate(self.date, 1)
    #     printInfo(f"{download_config.num_threads} threads are used to collect {content} ranking")
    #     with futures.ThreadPoolExecutor(download_config.num_threads) as executor:
    #         with tqdm.trange(len(urls), desc=f"Collecting {self.content} ids") as pbar:
    #             additional_headers = [
    #                 {
    #                     "Referer": re.search("(.*)&p", url).group(1),
    #                     "x-requested-with": "XMLHttpRequest",
    #                     "COOKIE": user_config.cookie,
    #                 }
    #                 for url in urls
    #             ]
    #             image_ids_futures = []
                
    #             # Check if content is "novel", use selectRankingPage for novels
    #             if self.content == "novel":
    #                 printInfo(f"Collecting {self.content} {urls} ranking page...")
    #                 image_ids_futures = [
    #                     executor.submit(collect, url, selectRankingPage, additional_header)
    #                     for url, additional_header in zip(urls, additional_headers)
    #                 ]
    #             else:
    #                 printInfo(f"Collecting {self.content} ranking...")
    #                 image_ids_futures = [
    #                     executor.submit(collect, url, selectRanking, additional_header)
    #                     for url, additional_header in zip(urls, additional_headers)
    #                 ]

    #             for future in futures.as_completed(image_ids_futures):
    #                 image_ids = future.result()
    #                 if image_ids is not None:
    #                     self.collector.add(image_ids)
    #                 pbar.update()

    #     printInfo(f"===== Collect {content} ranking complete =====")

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
