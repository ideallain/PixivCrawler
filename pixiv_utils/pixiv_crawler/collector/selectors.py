import json
import re
from typing import List, Set

from bs4 import BeautifulSoup
from requests.models import Response

from pixiv_utils.pixiv_crawler.utils import writeFailLog
from pixiv_utils.pixiv_crawler.utils import printInfo


def selectTag(response: Response) -> List[str]:
    """
    Collect all tags from (artwork.html)
    Sample url: https://www.pixiv.net/artworks/xxxxxx

    Returns:
        List[str]: tags
    """
    result = re.search(r"artworks/(\d+)", response.url)
    assert result is not None, f"Bad response in selectTag for URL: {response.url}"

    illust_id = result.group(1)
    content = json.loads(
        BeautifulSoup(response.text, "html.parser").find(id="meta-preload-data").get("content")
    )

    return [
        tag["translation"]["en"] if "translation" in tag else tag["tag"]
        for tag in content["illust"][illust_id]["tags"]["tags"]
    ]


def selectPage(response: Response) -> Set[str]:
    """
    Collect all image urls from (page.json)
    Sample url: https://www.pixiv.net/ajax/illust/xxxx/pages?lang=zh

    Returns:
        Set[str]: urls
    """
    group = set()
    for url in response.json()["body"]:
        group.add(url["urls"]["original"])
    return group


def selectRanking(response: Response) -> Set[str]:
    """
    Collect all illust_id (image_id) from (ranking.json)
    Sample url: https://www.pixiv.net/ranking.php?mode=daily&date=20200801&p=1&format=json

    Returns:
        Set[str]: illust_id (image_id)
    """
    printInfo("===== [selectRanking] =====")
    printInfo(response.url)
    image_ids = [artwork["illust_id"] for artwork in response.json()["contents"]]
    return set(map(str, image_ids))

# def selectRankingPage(response: Response) -> Set[str]:
#     """
#     Collect all illust_id (image_id) from ranking page (novel ranking) HTML content
#     Sample url: https://www.pixiv.net/novel/ranking.php?mode=daily&content=novel&date=20230801&p=1

#     Returns:
#         Set[str]: illust_id (image_id)
#     """
#     # 使用正则表达式提取 illust_id
#     print("[DEBUG] 进入 selectRankingPage 函数")
#     image_ids = set()
#     soup = BeautifulSoup(response.text, 'html.parser')

#     # 获取每个作品的链接，假设这些链接包含在小说页面的 <a> 标签中
#     for link in soup.find_all('a', href=True):
#         match = re.search(r"show.php\?id=(\d+)", link['href'])
#         if match:
#             image_ids.add(match.group(1))
    
#         # 保存到 JSON 文件
#     file_path = "novel_image_ids.json"
#     with open(file_path, "w", encoding="utf-8") as f:
#         json.dump(list(image_ids), f, indent=4, ensure_ascii=False)

#     print(f"Saved image IDs to {file_path}")

#     return image_ids
def selectRankingPage(response: Response) -> Set[str]:
    """
    Collect all illust_id (image_id) from ranking page (novel ranking) HTML content
    Sample url: https://www.pixiv.net/novel/ranking.php?mode=daily&content=novel&date=20230801&p=1

    Returns:
        Set[str]: illust_id (image_id)
    """

    image_ids = set()
    soup = BeautifulSoup(response.text, 'html.parser')

    # 获取每个作品的链接，匹配 "show.php?id=xxxx"
    for link in soup.find_all('a', href=True):
        match = re.search(r"show\.php\?id=(\d+)", link['href'])
        if match:
            image_ids.add(match.group(1))

    # 不在这里写文件，而是返回给上层使用
    return image_ids

def selectUser(response: Response) -> Set[str]:
    """
    Collect all illust_id (image_id) from (user.json)
    Sample url: https://www.pixiv.net/ajax/user/23945843/profile/all?lang=zh

    Returns:
        Set[str]: illust_id (image_id)
    """
    return set(response.json()["body"]["illusts"].keys())


def selectBookmark(response: Response) -> Set[str]:
    """
    Collect all illust_id (image_id) from (bookmark.json)
    Sample url: https://www.pixiv.net/ajax/user/xxx/illusts/bookmarks?tag=&offset=0&limit=48&rest=show&lang=zh

    Returns:
        Set[str]: illust_id (image_id)
    """
    # NOTE: id of disabled artwork is int (not str)
    id_group: Set[str] = set()
    for artwork in response.json()["body"]["works"]:
        illust_id = artwork["id"]
        if isinstance(illust_id, str):
            id_group.add(artwork["id"])
        else:
            writeFailLog(f"Disabled artwork {illust_id}.")
    return id_group


def selectKeyword(response: Response) -> Set[str]:
    """
    Collect all illust_id (image_id) from (keyword.json)
    Sample url: https://www.pixiv.net/ajax/search/artworks/{xxxxx}?word={xxxxx}&order=popular_d&mode=all&p=1&s_mode=s_tag_full&type=all&lang=zh"

    Returns:
        Set[str]: illust_id (image_id)
    """
    # NOTE: id of disable artwork is int (not str)
    id_group: Set[str] = set()
    for artwork in response.json()["body"]["illustManga"]["data"]:
        id_group.add(artwork["id"])
    return id_group

# def selectRecommends(response: Response) -> tuple:
#     """
#     Extract novel_ids and series_ids from a tag recommendation page
#     Sample URL: https://www.pixiv.net/tags/{tag}/novels?gs=1&p={page}
    
#     Returns:
#         tuple: (novel_ids, series_ids, has_next_page)
#     """
#     novel_ids = set()
#     series_ids = set()
    
#     soup = BeautifulSoup(response.text, 'html.parser')
#     print(response.text)
    
#     # Find all novel elements 
#     novel_elements = soup.select('[data-gtm-novel-id]')
    
#     for element in novel_elements:
#         # Extract novel ID
#         novel_id = element.get('data-gtm-novel-id')
#         if novel_id:
#             novel_ids.add(novel_id)
            
#         # Extract series ID if present
#         series_id = element.get('data-gtm-series-id')
#         if series_id and series_id != "0":  # Skip 0 which indicates no series
#             series_ids.add(series_id)
    
#     # Check if there's a next page
#     next_button = soup.select_one('a[rel="next"]')
#     has_next_page = next_button is not None
    
#     printInfo(f"Found {len(novel_ids)} novel IDs and {len(series_ids)} series IDs")
#     return novel_ids, series_ids, has_next_page
# def selectRecommends(response: Response) -> tuple:
#     """
#     Extract novel_ids and series_ids from a tag recommendation page JSON response
#     Sample URL: https://www.pixiv.net/tags/{tag}/novels?gs=1&p={page}
    
#     Returns:
#         tuple: (novel_ids, series_ids, has_next_page)
#     """
#     novel_ids = set()
#     series_ids = set()
    
#     try:
#         # Parse the JSON response
#         data = response.json()
        
#         # Check if the response structure is as expected
#         if "error" in data and not data["error"] and "body" in data and "novel" in data["body"] and "data" in data["body"]["novel"]:
#             novels_data = data["body"]["novel"]["data"]
            
#             # Extract novel IDs
#             for novel in novels_data:
#                 if "id" in novel:
#                     novel_ids.add(novel["id"])
                
#                 # Some novels might be part of a series, extract series IDs if available
#                 # Note: we would need to check the actual JSON structure to locate series IDs
#                 # For now, assuming it might be in a field like "seriesId" or "series_id"
#                 if "seriesId" in novel and novel["seriesId"] and novel["seriesId"] != "0":
#                     series_ids.add(novel["seriesId"])
#                 elif "series_id" in novel and novel["series_id"] and novel["series_id"] != "0":
#                     series_ids.add(novel["series_id"])
            
#             # Check for pagination - based on the total number of novels vs current page size
#             has_next_page = False
#             if "total" in data["body"]["novel"] and len(novels_data) > 0:
#                 total_novels = data["body"]["novel"]["total"]
#                 has_next_page = total_novels > len(novels_data)
            
#             printInfo(f"Found {len(novel_ids)} novel IDs and {len(series_ids)} series IDs")
#             return novel_ids, series_ids, has_next_page
    
#     except Exception as e:
#         printInfo(f"Error processing recommendations JSON: {e}")
    
#     # Return empty sets if processing failed
#     return set(), set(), False
# def selectRecommends(response: Response) -> tuple:
#     """
#     Collect all novel_ids from tag recommendation page (JSON response)
#     Sample URL: https://www.pixiv.net/ajax/search/novels/{tag}?word={tag}&order=date_d&...
    
#     Returns:
#         tuple: (novel_ids, series_ids, has_next_page)
#     """
#     novel_ids = set()
#     series_ids = set()
#     has_next_page = False
    
#     try:
#         data = response.json()
        
#         # 检查响应结构并提取数据
#         if not data.get("error", True) and "body" in data and "novel" in data["body"] and "data" in data["body"]["novel"]:
#             novels_data = data["body"]["novel"]["data"]
            
#             # 提取小说ID
#             for novel in novels_data:
#                 if "id" in novel:
#                     novel_ids.add(novel["id"])
            
#             # 检查分页信息
#             if "novel" in data["body"] and "total" in data["body"]["novel"] and "lastPage" in data["body"]["novel"]:
#                 total = data["body"]["novel"]["total"]
#                 last_page = data["body"]["novel"]["lastPage"]
                
#                 # 从URL获取当前页码
#                 current_page = 1
#                 page_match = re.search(r"p=(\d+)", response.url)
#                 if page_match:
#                     current_page = int(page_match.group(1))
                
#                 # 检查是否还有下一页
#                 has_next_page = current_page < last_page
            
#             printInfo(f"Found {len(novel_ids)} novel IDs from page, has_next_page: {has_next_page}")
    
#     except Exception as e:
#         printInfo(f"Error processing recommendations JSON: {e}")
    
#     return novel_ids, series_ids, has_next_page

def selectRecommends(response: Response):
    """
    Extract novel data from tag recommendation page JSON response
    Sample URL: https://www.pixiv.net/ajax/search/novels/{tag}?word={tag}&order=date_d&mode=all&p={page}&...
    
    Returns:
        The body part of the JSON data, or None if there was an error
    """
    try:
        data = response.json()
        
        # 检查响应结构
        if not data.get("error", True) and "body" in data:
            return data["body"]
        else:
            printInfo(f"Error in API response: {data.get('message', 'Unknown error')}")
    
    except Exception as e:
        printInfo(f"Error processing recommendations JSON: {e}")
    
    return None