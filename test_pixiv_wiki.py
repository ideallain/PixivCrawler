import requests
from bs4 import BeautifulSoup
import re

# ------------- 常量定义 -------------
TYPE_MAIN_TITLE = "entry"
TYPE_MAIN_IMAGE_URL = "cover"
TYPE_TITLE = "h2"
TYPE_SUBTITLE = "h3"  # 可以细分为 h3/h4, 这里仅示例
TYPE_PARAGRAPH = "p"
TYPE_IMAGE = "img"
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
        给定一个词条名称(如"Original")，抓取对应页面并解析，返回 JSON 风格的 list(dict)。
        """
        encoded_title = self._encode_title(article_title)
        full_url = self.base_url + encoded_title
        # full_url = self.base_url + article_title

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
        # 伪装成浏览器的请求头(可根据需要改动)
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
            # 发起请求
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.text
        except:
            return None


    def _fetch_html(self, url: str):
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            return resp.text
        except:
            return None

    # ========== 2) 内部方法：解析 HTML ==========

    def _parse_html(self, html: str):
        soup = BeautifulSoup(html, "html.parser")

        # 主体 <article>：
        article = soup.find("article")
        if not article:
            print("no ariticle")
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

        # (B) 从正文提取：data-header-id="h2_xxx"（一级小标题）、h3_xxx（二级小标题）
        #     以及段落 <p>、内部的列表 <ul>
        #     这里让 parse_sections 递归/遍历即可
        results.extend(self._parse_sections(article))

        # (C) 解析底部（或者其他区域）相关条目(Parent / Child / Sibling)
        related_data = self._parse_related_articles(article)
        if related_data:
            results.append(related_data)

        return results

    def _parse_sections(self, parent):
        """
        递归/或分层解析 <div data-header-id="h2_xxx"> ... <h2>...<p>...</h3>... 
        一些页面还有 data-header-id="h3_xxx"，可以都捕捉
        """
        output = []

        # --- 先匹配 h2 区块 ---
        # data-header-id="h2_0", "h2_1" 等
        h2_sections = parent.find_all("div", attrs={"data-header-id": re.compile(r"^h2_\d+$")})
        for sec in h2_sections:
            # 取得 <h2> 文本 => TYPE_TITLE
            h2_tag = sec.find("h2")
            if h2_tag:
                text_h2 = h2_tag.get_text(strip=True)
                if text_h2:
                    output.append({"type": TYPE_TITLE, "text": text_h2})

            # 解析所有 <p>
            for p_tag in sec.find_all("p", recursive=False):
                p_text = p_tag.get_text(strip=True)
                if p_text:
                    output.append({"type": TYPE_PARAGRAPH, "text": p_text})

            # 解析 <ul> 列表
            for ul_tag in sec.find_all("ul", recursive=False):
                list_data = self._parse_list(ul_tag)
                if list_data and list_data["list_items"]:
                    output.append(list_data)

            # 也许里头还有 h3
            h3_sections = sec.find_all("div", attrs={"data-header-id": re.compile(r"^h3_\d+$")})
            for h3_sec in h3_sections:
                # <h3>
                h3_tag = h3_sec.find("h3")
                if h3_tag:
                    text_h3 = h3_tag.get_text(strip=True)
                    if text_h3:
                        # 这里也可存成 TYPE_SUBTITLE
                        output.append({"type": TYPE_SUBTITLE, "text": text_h3})

                # h3 下所有 <p>
                for p_tag in h3_sec.find_all("p", recursive=False):
                    p_text = p_tag.get_text(strip=True)
                    if p_text:
                        output.append({"type": TYPE_PARAGRAPH, "text": p_text})

                # h3 下的 <ul>
                for ul_tag in h3_sec.find_all("ul", recursive=False):
                    list_data = self._parse_list(ul_tag)
                    if list_data and list_data["list_items"]:
                        output.append(list_data)

        return output

    def _parse_list(self, ul):
        """
        解析 <ul> -> list_items
        """
        items = []
        li_tags = ul.find_all("li", recursive=False)
        for li in li_tags:
            text_li = li.get_text(strip=True)
            if text_li:
                items.append({"type": TYPE_LIST_ITEM, "text": text_li})
        return {
            "type": TYPE_LIST,
            "list_type": LIST_TYPE_BULLETED,
            "list_items": items
        }

    # ========== 3) 解析“Related Articles”(Parent / Child / Sibling) ==========

    def _parse_related_articles(self, article):
        """
        在底部 <section class="mt-24"> 等处，有 <h3>Parent Article</h3>, <h3>Child Article</h3>, <h3>Sibling Article</h3>
        以及对应列表 (div / ul) 里的项目
        """
        sec = article.find("section", class_="mt-24")
        if not sec:
            return None

        # 返回结构
        related = {
            "type": TYPE_RELATED_ARTICLES,
            "parent": [],
            "child": [],
            "sibling": []
        }

        # 找所有 <h3> = "Parent Article"/"Child Article"/"Sibling Article"
        h3_list = sec.find_all("h3")
        for h3 in h3_list:
            h3_text = h3.get_text(strip=True)

            # 可能是 "Parent Article" / "Child Article" / "Sibling Article"
            next_elem = h3.find_next_sibling(["div", "ul"])
            if not next_elem:
                next_elem = h3.find_next_siblings(["div", "ul"], limit=1)
                next_elem = next_elem[0] if next_elem else None

            items = self._parse_related_list(next_elem) if next_elem else []

            if "Parent" in h3_text:
                related["parent"] = items
            elif "Child" in h3_text:
                related["child"] = items
            elif "Sibling" in h3_text:
                related["sibling"] = items

        return related

    def _parse_related_list(self, container):
        """
        解析 <div> 或 <ul> 下的一系列子项目，每个项目通常包含一个 <a data-gtm-class="article-bottom_**-article"> 
        + <img> 
        """
        results = []
        if not container:
            return results

        if container.name == "div":
            # 可能只有一个或一个 box
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
        # 先找到 a 标签(带 data-gtm-class="article-bottom_{parent,child,sibling}-article") 或简单判断
        # 其实还可以直接找 a 并判断 gtm-class 里含 "..._article"
        re_pattern = re.compile(r"article-bottom_.*-article")

        a_tag = sel.find("a", attrs={"data-gtm-class": re_pattern})
        if not a_tag:
            # 也有可能没有 data-gtm-class? 需按需改
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


if __name__ == "__main__":
    parser = PixivDicParser()
    article_name = "coupling" #"レグルス・コルニアス"   # 示例: 解析“Original”页面
    data = parser.fetch_article_elements(article_name)

    import json
    print(json.dumps(data, indent=2, ensure_ascii=False))
