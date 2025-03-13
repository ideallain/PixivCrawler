import requests
from bs4 import BeautifulSoup
import re

# ------------- 常量定义 -------------
TYPE_MAIN_TITLE = "entry"
TYPE_MAIN_IMAGE_URL = "cover"
TYPE_TITLE = "h2"      # For demonstration, treat as "h2"
TYPE_SUBTITLE = "h3"   # For demonstration, treat as "h3"
TYPE_PARAGRAPH = "p"
TYPE_LIST = "ul"
TYPE_LIST_ITEM = "li"
TYPE_RELATED_ARTICLES = "ref"

LIST_TYPE_BULLETED = "bl"
LIST_TYPE_NUMBERED = "ol"


class PixivDicParser:
    def __init__(self, base_url="https://dic.pixiv.net/en/a/"):
        self.base_url = base_url

    def fetch_article_elements(self, article_title: str):
        """
        给定一个词条名称(如"Original")，抓取对应页面并解析，返回 JSON 风格的 nested list(dict)。
        """
        encoded_title = self._encode_title(article_title)
        full_url = self.base_url + encoded_title

        html_content = self._fetch_pixiv_page(full_url)
        if not html_content:
            raise Exception(f"Empty or invalid content from {full_url}.")

        elements = self._parse_html(html_content)
        return elements

    # ========== 1) 内部方法：下载页面 ==========
    def _encode_title(self, title: str) -> str:
        from urllib.parse import quote
        return quote(title)

    def _fetch_pixiv_page(self, url: str):
        """
        从环境变量中读取所有 header（如 PIXIV_COOKIE, PIXIV_USER_AGENT 等）。
        如果未在 .env 或系统环境变量中设置，则使用默认值。
        """
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/133.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8,"
                "application/signed-exchange;v=b3;q=0.7"
            ),
            "Accept-Language": "en",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            # 这里直接把你给出的 Cookie 字符串整体放进去
            "Cookie": (
                "p_ab_d_id=1805382873; _ga=GA1.1.45655621.1738591759; p_ab_id=5; "
                "p_ab_id_2=7; PHPSESSID=113362431_kvioJgDvQxIPgUsLOWYI1xO5VKIkyDjr; "
                "device_token=18bc3f60e1e5d47ce7b2ffb15539da68; privacy_policy_agreement=7; "
                "_ga_MZ1NL4PHH0=GS1.1.1738648321.1.1.1738648660.0.0.0; c_type=42; "
                "privacy_policy_notification=0; a_type=0; b_type=0; "
                "_gcl_au=1.1.736877511.1738654534; default_service_is_touch=no; "
                "dic_page_view_is_touch=no; p_ab_d_id_expire=1742643995; dic_pixiv_uid=113362431; "
                "_ga_75BBYNYN9J=GS1.1.1740918606.21.0.1740918617.0.0.0; "
                "dic_news_read=2Lx8TGY7lSrXqm8JA6hdzo,2h758duYptFHiPOwyoLeZY; "
                "yuid_b=KFgXljA; __cf_bm=N8_na4g41ofXykN5OA_lcXivMwTlhXT1O.YVS7Y8pwQ-1740979335-1.0.1.1-"
                "BbGUvhWo5D4WprdJo.MQK773PWT8abKIAxPwLjzj2RlpgpnceJDXNeJHWE1Flh9xjcOi93ishnEpxPoGQtVTK."
                "QDpYNqsIkKX_NvQoOaftw4WgmS4QfBmHT1BI2PHD4Y; pixpsession2=4de50d87ede92b35eb893396e594092e; "
                "cf_clearance=_tCGr5rWnoDwWRWr.xWnWibg7h8Ro6VRp_kzEI97ZG8-1740980048-1.2.1.1-W_J6."
                "EHhNSlKdQSSIuOaLMroWhH.INWIlLePGxWbh4R2zUlivvCwGm5SO.fcly6pMFphT8inGAdxLcbI_QRFkPiNwkOfK"
                "lKtcMGhs9EfTCG2rRFZOpnwU.RIlnK0NpOyEO2BDOblsaPnEvU5wSC2V1vC6ZZ03du.nUzxkzcIb0vDQ8nvv7GED"
                "oWGtm7Fr6ubyYjYlsOeT58GjK.TDZpY3RO3ws0WO.ao45tEiw_7CmMmlEFJgUS7SiLMsp6XSdjgZnxm93iMtYSlh6"
                "AMHd12EJRUGgUn1_PU8l5cEs91Yh4k8tFHrwAlNBBwc9jFQT2aWYdKnZDUXORBc6scwtzGE8Og7OneX_vmJS1bUb"
                "wnQP8rLju6rqq4s5eBjSUtowzxQkglAQ4lY3OW3UhJkxhoDqZkqzLqZHZPDOLQuYfjPuY; "
                "_ga_ZQEKG3EF2C=GS1.1.1740970382.4.1.1740980056.49.0.0"
            )
        }
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"[ERROR] _fetch_pixiv_page failed: {e}")
            return None

    # ========== 2) 内部方法：解析 HTML ==========

    def _parse_html(self, html: str):
        soup = BeautifulSoup(html, "html.parser")

        # 主体 <article>：
        article = soup.find("article")
        if not article:
            print("no article found")
            return []

        results = []

        # (A) 解析 header：主图 & 主标题
        header = article.find("header", id="article-content-header")
        if header:
            # -- 主图：
            main_img_tag = header.find("img")
            if main_img_tag:
                main_img_url = main_img_tag.get("src", "")
                if main_img_url:
                    results.append({
                        "type": TYPE_MAIN_IMAGE_URL,
                        "url": main_img_url
                    })

            # -- 主标题：
            h1 = header.find("h1", id="article-name")
            if h1:
                main_title_text = h1.get_text(strip=True)
                if main_title_text:
                    results.append({
                        "type": TYPE_MAIN_TITLE,
                        "text": main_title_text
                    })

        # (B) 从正文提取 h2/h3 块及其子节点
        body_data = self._parse_sections(article)
        if body_data:
            results.extend(body_data)

        # (C) 解析底部相关条目(Parent / Child / Sibling)
        related_data = self._parse_related_articles(article)
        if related_data:
            results.append(related_data)

        return results

    def _parse_sections(self, parent):
        """
        解析 h2/h3 区块，并将同级别的 p / ul 等作为其子节点，而不是扁平化输出。
        结构类似：
        [
          {
            "type": "h2",
            "text": "Heading Text",
            "children": [
              {"type": "p", "text": "..."},
              {"type": "ul", "list_items": [...]},
              {
                "type": "h3",
                "text": "Subheading text",
                "children": [
                  {"type": "p", "text": "..."},
                  ...
                ]
              }
            ]
          },
          ...
        ]
        """
        output = []
        # 找到所有 data-header-id="h2_xxx" 的 div
        h2_sections = parent.find_all("div", attrs={"data-header-id": re.compile(r"^h2_\d+$")})

        for sec in h2_sections:
            # 1) 创建 h2 对象
            h2_obj = None
            h2_tag = sec.find("h2")
            if h2_tag:
                text_h2 = h2_tag.get_text(strip=True)
                if text_h2:
                    h2_obj = {
                        "type": TYPE_TITLE,  # or "h2"
                        "text": text_h2,
                        "children": []
                    }
            else:
                # 若没找到 h2，仍然构造一个空节点
                h2_obj = {
                    "type": TYPE_TITLE,
                    "text": "",
                    "children": []
                }

            # 2) 解析 h2 下直系子节点：p, ul, div(h3)
            for child in sec.find_all(["p", "ul", "div"], recursive=False):
                if child.name == "p":
                    p_text = child.get_text(strip=True)
                    if p_text:
                        h2_obj["children"].append({
                            "type": TYPE_PARAGRAPH,
                            "text": p_text
                        })
                elif child.name == "ul":
                    list_data = self._parse_list(child)
                    if list_data and list_data["list_items"]:
                        h2_obj["children"].append(list_data)
                elif child.name == "div":
                    # 可能是 data-header-id="h3_xxx" 
                    h3_data_header = child.get("data-header-id", "")
                    if re.match(r"^h3_\d+$", h3_data_header):
                        h3_obj = self._parse_h3_section(child)
                        if h3_obj:
                            h2_obj["children"].append(h3_obj)
                    else:
                        # 不是 h3，就暂时忽略或做别的处理
                        pass

            # 3) 将此 h2_obj 添加到 output
            if h2_obj:
                output.append(h2_obj)

        return output

    def _parse_h3_section(self, h3_div):
        """
        给定 <div data-header-id="h3_xxx">，解析其中的 h3 文本，以及所有 <p>, <ul> 作为 children。
        返回:
        {
          "type": "h3",
          "text": "Subheading",
          "children": [
            {... p ...},
            {... ul ...}
          ]
        }
        """
        h3_tag = h3_div.find("h3", recursive=False)
        if not h3_tag:
            return None

        text_h3 = h3_tag.get_text(strip=True)
        if not text_h3:
            return None

        h3_obj = {
            "type": TYPE_SUBTITLE,  # or "h3"
            "text": text_h3,
            "children": []
        }

        # 解析直系子节点 p / ul
        for child in h3_div.find_all(["p", "ul"], recursive=False):
            if child.name == "p":
                p_text = child.get_text(strip=True)
                if p_text:
                    h3_obj["children"].append({
                        "type": TYPE_PARAGRAPH,
                        "text": p_text
                    })
            elif child.name == "ul":
                list_data = self._parse_list(child)
                if list_data and list_data["list_items"]:
                    h3_obj["children"].append(list_data)

        return h3_obj

    def _parse_list(self, ul):
        """
        解析 <ul> -> {"type": "ul", "list_type": "bl", "list_items": [{...}, ...]}
        """
        items = []
        li_tags = ul.find_all("li", recursive=False)
        for li in li_tags:
            text_li = li.get_text(strip=True)
            if text_li:
                items.append({"type": TYPE_LIST_ITEM, "text": text_li})
        return {
            "type": TYPE_LIST,
            "list_type": LIST_TYPE_BULLETED,  # or check if it's numbered
            "list_items": items
        }

    # ========== 3) 解析“Related Articles”==========

    def _parse_related_articles(self, article):
        """
        在底部 <section class="mt-24"> 等处，有 <h3>Parent Article</h3>, <h3>Child Article</h3>, <h3>Sibling Article</h3>
        以及对应列表 (div / ul) 里的项目
        """
        sec = article.find("section", class_="mt-24")
        if not sec:
            return None

        related = {
            "type": TYPE_RELATED_ARTICLES,
            "parent": [],
            "child": [],
            "sibling": []
        }

        h3_list = sec.find_all("h3")
        for h3 in h3_list:
            h3_text = h3.get_text(strip=True)
            next_elem = h3.find_next_sibling(["div", "ul"])
            if not next_elem:
                next_elem_list = h3.find_next_siblings(["div", "ul"], limit=1)
                next_elem = next_elem_list[0] if next_elem_list else None

            items = self._parse_related_list(next_elem) if next_elem else []
            if "Parent" in h3_text:
                related["parent"] = items
            elif "Child" in h3_text:
                related["child"] = items
            elif "Sibling" in h3_text:
                related["sibling"] = items

        return related

    def _parse_related_list(self, container):
        results = []
        if not container:
            return results

        if container.name == "div":
            item = self._parse_related_item(container)
            if item:
                results.append(item)
        elif container.name == "ul":
            li_tags = container.find_all("li", recursive=False)
            for li in li_tags:
                item = self._parse_related_item(li)
                if item:
                    results.append(item)

        return results

    def _parse_related_item(self, sel):
        re_pattern = re.compile(r"article-bottom_.*-article")
        a_tag = sel.find("a", attrs={"data-gtm-class": re_pattern})
        if not a_tag:
            return None

        title_text = a_tag.get_text(strip=True)
        link_url = a_tag.get("href", "")
        img_tag = sel.find("img")
        img_url = img_tag.get("src") if img_tag else ""

        return {
            "title": title_text,
            "link": link_url,
            "img": img_url
        }


class WikiCrawler:
    """
    A simple crawler interface that wraps PixivDicParser
    to keep an API similar to your other crawlers (e.g., RankingCrawler).
    """
    def __init__(self, base_url="https://dic.pixiv.net/en/a/"):
        self.parser = PixivDicParser(base_url)

    def run(self, article_title: str = "Original"):
        """
        Fetch and parse a Pixiv dictionary article.
        Returns a nested list of dictionary elements describing the entry.
        """
        return self.parser.fetch_article_elements(article_title)


if __name__ == "__main__":
    # Example usage
    crawler = WikiCrawler()
    # data = crawler.run("レグルス・コルニアス")  # or some other term
    data = crawler.run("Regulus Corneas")  # or some other term
    import json
    print(json.dumps(data, indent=2, ensure_ascii=False))