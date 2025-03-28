import datetime
import os

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


def downloadRanking(recommends_tags=None, max_pages=5):
    """
    Download artworks from rankings

    NOTE: Require cookie for R18 images!

    Args:
        capacity (int): flow capacity, default is 1024MB
    """
    # user_config.user_id = ""
    # user_config.cookie = ""
    download_config.with_tag = False
    download_config.num_threads = 12

    if recommends_tags:
        print("running in recommend_tags mode")
    else:
        ranking_config.start_date = datetime.date(2024, 2, 6)
        ranking_config.range = 30
        ranking_config.mode = "daily"
        ranking_config.content_mode = "novel"
        ranking_config.num_artwork = 50

    displayAllConfig()
    checkDir(download_config.store_path)

    app = RankingCrawler(capacity=200,recommends_tags=recommends_tags, max_pages=max_pages)
    app.run()


def downloadBookmark():
    """
    Download artworks from bookmark

    NOTE: Require cookie!

    Args:
        n_images (int): max download number, default is 200
        capacity (int): flow capacity, default is 1024MB
    """
    download_config.with_tag = False
    # user_config.user_id = "[TODO]: Your user_id here"
    # user_config.cookie = "[TODO]: Your cookie here"

    displayAllConfig()
    checkDir(download_config.store_path)

    app = BookmarkCrawler(n_images=20, capacity=200)
    app.run()


def downloadUser():
    """
    Download artworks from a single artist

    NOTE: Require cookie for R18 images!

    Args:
        artist_id (str): artist id
        capacity (int): flow capacity, default is 1024MB
    """
    # user_config.user_id = ""
    # user_config.cookie = ""
    download_config.with_tag = False

    displayAllConfig()
    checkDir(download_config.store_path)

    app = UserCrawler(artist_id="16552303", capacity=200)
    app.run()


def downloadKeyword():
    """
    Download search results of a keyword (sorted by popularity if order=True)
    Support advanced search, e.g. "(Lucy OR 边缘行者) AND (5000users OR 10000users)", refer to https://www.pixiv.help/hc/en-us/articles/235646387-I-would-like-to-know-how-to-search-for-content-on-pixiv

    NOTE: Require cookie for R18 images!
    NOTE: Require premium account for popularity sorting!

    Args:
        keyword (str): search keyword
        order (bool): order by popularity or not, default is False
        mode (str): content mode, default is "safe", support ["safe", "r18", "all"]
        n_images (int): max download number, default is 200
        capacity (int): flow capacity, default is 1024MB
    """
    # user_config.user_id = ""
    # user_config.cookie = ""
    download_config.with_tag = False

    displayAllConfig()
    checkDir(download_config.store_path)

    app = KeywordCrawler(
        keyword="(鍾タル 体調不良 SS) AND (5000users OR 10000users)",
        order=False,
        mode=["safe", "r18", "all"][-1],
        n_images=20,
        capacity=200,
    )
    app.run()


def loadEnv():
    """
    Load environment variables for proxy, cookie, and user_id
    """
    # Use system proxy settings
    proxy = os.getenv("https_proxy") or os.getenv("HTTPS_PROXY")
    if proxy is not None:
        network_config.proxy["https"] = proxy

    # Use system user_id and cookie
    cookie = os.getenv("PIXIV_COOKIE")
    uid = os.getenv("PIXIV_UID")
    if cookie is not None:
        user_config.cookie = cookie
    if uid is not None:
        user_config.user_id = uid


if __name__ == "__main__":
    import argparse
    
    loadEnv()
    
    parser = argparse.ArgumentParser(description="Pixiv Crawler")
    parser.add_argument("--ranking", action="store_true", help="Download from rankings")
    parser.add_argument("--bookmark", action="store_true", help="Download from bookmarks")
    parser.add_argument("--user", type=str, help="Artist ID to download from")
    parser.add_argument("--keyword", type=str, help="Search keyword to download from")
    parser.add_argument("--recommends", nargs="+", help="Tags for recommendations to crawl")
    parser.add_argument("--max-pages",type=int,default=5,help="Maximum number of pages matched tag to crawl")
    
    args = parser.parse_args()
    
    if args.ranking:
        downloadRanking()
    elif args.bookmark:
        downloadBookmark()
    elif args.user:
        # Update UserCrawler to use the provided ID
        app = UserCrawler(artist_id=args.user, capacity=200)
        app.run()
    elif args.keyword:
        # Update KeywordCrawler to use the provided keyword
        app = KeywordCrawler(keyword=args.keyword, order=False, mode="all", n_images=20, capacity=200)
        app.run()
    elif args.recommends:
        # Use the modified RankingCrawler with recommends mode
        downloadRanking(recommends_tags=args.recommends, max_pages=args.max_pages)
    else:
        # Default behavior if no options provided
        downloadRanking()
