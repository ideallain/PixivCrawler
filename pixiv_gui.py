import tkinter as tk
from tkinter import messagebox
import threading
import datetime
import os

# 从 pixiv_utils.pixiv_crawler 导入相关爬虫和配置
from pixiv_utils.pixiv_crawler import (
    BookmarkCrawler,
    KeywordCrawler,
    RankingCrawler,
    UserCrawler,
    checkDir,
    displayAllConfig,
    download_config,
    network_config,
    ranking_config,
    user_config,
)

def loadEnv():
    """
    加载环境变量，如果存在则优先使用
    """
    proxy = os.getenv("https_proxy") or os.getenv("HTTPS_PROXY")
    if proxy is not None:
        network_config.proxy["https"] = proxy

    cookie = os.getenv("PIXIV_COOKIE")
    uid = os.getenv("PIXIV_UID")
    if cookie is not None:
        user_config.cookie = cookie
    if uid is not None:
        user_config.user_id = uid

def downloadRanking(recommends_tags=None, max_pages=5):
    """
    下载排行榜作品
    如果传入 recommends_tags，则以推荐模式运行，否则使用默认排行榜设置
    """
    download_config.with_tag = False
    download_config.num_threads = 12

    if recommends_tags:
        print("running in recommend_tags mode")
    else:
        ranking_config.start_date = datetime.date(2024, 2, 6)
        ranking_config.range = 360
        ranking_config.mode = "weekly"
        ranking_config.content_mode = "novel"
        ranking_config.num_artwork = 50

    displayAllConfig()
    checkDir(download_config.store_path)

    app = RankingCrawler(capacity=200, recommends_tags=recommends_tags, max_pages=max_pages)
    app.run()

def downloadBookmark():
    """
    下载收藏夹作品
    """
    download_config.with_tag = False
    displayAllConfig()
    checkDir(download_config.store_path)

    app = BookmarkCrawler(n_images=20, capacity=200)
    app.run()

def downloadUser(artist_id):
    """
    下载指定用户的作品
    """
    download_config.with_tag = False
    displayAllConfig()
    checkDir(download_config.store_path)

    app = UserCrawler(artist_id=artist_id, capacity=200)
    app.run()

def downloadKeyword(keyword):
    """
    根据关键字下载搜索结果
    """
    download_config.with_tag = False
    displayAllConfig()
    checkDir(download_config.store_path)

    app = KeywordCrawler(keyword=keyword, order=False, mode="all", n_images=20, capacity=200)
    app.run()

class PixivCrawlerGUI:
    def __init__(self, master):
        self.master = master
        master.title("Pixiv Crawler GUI")

        # 模式选择区域
        self.mode_var = tk.StringVar(value="ranking")
        self.radio_frame = tk.Frame(master)
        self.radio_frame.pack(pady=10)

        tk.Label(self.radio_frame, text="选择下载模式：").pack(anchor="w")
        modes = [
            ("排行榜", "ranking"),
            ("收藏", "bookmark"),
            ("指定用户", "user"),
            ("关键字搜索", "keyword"),
            ("排行榜（推荐）", "recommends")
        ]
        for text, mode in modes:
            tk.Radiobutton(self.radio_frame, text=text, variable=self.mode_var, value=mode, command=self.update_fields).pack(anchor="w")

        # 参数输入区域
        self.param_frame = tk.Frame(master)
        self.param_frame.pack(pady=10)

        # 指定用户时的 Artist ID
        self.user_frame = tk.Frame(self.param_frame)
        tk.Label(self.user_frame, text="Artist ID:").pack(side="left")
        self.user_entry = tk.Entry(self.user_frame)
        self.user_entry.pack(side="left")

        # 关键字搜索时的搜索关键字
        self.keyword_frame = tk.Frame(self.param_frame)
        tk.Label(self.keyword_frame, text="搜索关键字:").pack(side="left")
        self.keyword_entry = tk.Entry(self.keyword_frame)
        self.keyword_entry.pack(side="left")

        # 排行榜推荐时的推荐标签和最大页数
        self.recommends_frame = tk.Frame(self.param_frame)
        tk.Label(self.recommends_frame, text="推荐标签（逗号分隔）:").pack(side="left")
        self.tags_entry = tk.Entry(self.recommends_frame)
        self.tags_entry.pack(side="left")
        tk.Label(self.recommends_frame, text="最大页数:").pack(side="left")
        self.max_pages_entry = tk.Entry(self.recommends_frame, width=5)
        self.max_pages_entry.insert(0, "5")
        self.max_pages_entry.pack(side="left")

        # 开始下载按钮
        self.start_button = tk.Button(master, text="Start Download", command=self.start_download)
        self.start_button.pack(pady=10)

        # 环境配置区域（用于配置 Proxy、PIXIV_UID 和 PIXIV_COOKIE）
        self.config_frame = tk.LabelFrame(master, text="环境配置", padx=10, pady=10)
        self.config_frame.pack(padx=10, pady=10, fill="x")

        tk.Label(self.config_frame, text="Proxy (https):").grid(row=0, column=0, sticky="w")
        self.proxy_entry = tk.Entry(self.config_frame, width=50)
        self.proxy_entry.grid(row=0, column=1)
        self.proxy_entry.insert(0, os.getenv("https_proxy") or os.getenv("HTTPS_PROXY") or "")

        tk.Label(self.config_frame, text="PIXIV_UID:").grid(row=1, column=0, sticky="w")
        self.uid_entry = tk.Entry(self.config_frame, width=50)
        self.uid_entry.grid(row=1, column=1)
        self.uid_entry.insert(0, os.getenv("PIXIV_UID") or "")

        tk.Label(self.config_frame, text="PIXIV_COOKIE:").grid(row=2, column=0, sticky="w")
        self.cookie_entry = tk.Entry(self.config_frame, width=50)
        self.cookie_entry.grid(row=2, column=1)
        self.cookie_entry.insert(0, os.getenv("PIXIV_COOKIE") or "")

        self.save_button = tk.Button(self.config_frame, text="保存配置", command=self.update_config)
        self.save_button.grid(row=3, column=0, columnspan=2, pady=5)

        self.update_fields()

    def update_fields(self):
        """
        根据选择的模式显示或隐藏相关的参数输入框
        """
        mode = self.mode_var.get()
        self.user_frame.pack_forget()
        self.keyword_frame.pack_forget()
        self.recommends_frame.pack_forget()

        if mode == "user":
            self.user_frame.pack(pady=5)
        elif mode == "keyword":
            self.keyword_frame.pack(pady=5)
        elif mode == "recommends":
            self.recommends_frame.pack(pady=5)

    def update_config(self):
        """
        从 GUI 中读取配置并更新到对应的全局配置中
        """
        proxy = self.proxy_entry.get().strip()
        if proxy:
            network_config.proxy["https"] = proxy
        else:
            network_config.proxy["https"] = ""
        uid = self.uid_entry.get().strip()
        user_config.user_id = uid  # 可以为空
        cookie = self.cookie_entry.get().strip()
        user_config.cookie = cookie  # 可以为空
        messagebox.showinfo("配置", "环境配置已保存。")

    def start_download(self):
        """
        启动下载任务（在后台线程中执行以防止界面冻结）
        """
        # 先更新环境配置
        self.update_config()
        t = threading.Thread(target=self.run_download, args=(self.mode_var.get(),))
        t.start()

    def run_download(self, mode):
        try:
            # 如果需要，可以先调用 loadEnv()，这里已通过 GUI 更新配置
            # loadEnv()
            if mode == "ranking":
                downloadRanking()
            elif mode == "bookmark":
                downloadBookmark()
            elif mode == "user":
                artist_id = self.user_entry.get().strip()
                if not artist_id:
                    messagebox.showerror("错误", "请输入 Artist ID")
                    return
                downloadUser(artist_id)
            elif mode == "keyword":
                keyword = self.keyword_entry.get().strip()
                if not keyword:
                    messagebox.showerror("错误", "请输入搜索关键字")
                    return
                downloadKeyword(keyword)
            elif mode == "recommends":
                tags = self.tags_entry.get().strip()
                if not tags:
                    messagebox.showerror("错误", "请输入推荐标签")
                    return
                recommends_tags = [tag.strip() for tag in tags.split(",") if tag.strip()]
                try:
                    max_pages = int(self.max_pages_entry.get().strip())
                except ValueError:
                    messagebox.showerror("错误", "最大页数必须为整数")
                    return
                downloadRanking(recommends_tags=recommends_tags, max_pages=max_pages)
            messagebox.showinfo("完成", "下载任务已完成")
        except Exception as e:
            messagebox.showerror("错误", str(e))

if __name__ == "__main__":
    root = tk.Tk()
    gui = PixivCrawlerGUI(root)
    root.mainloop()