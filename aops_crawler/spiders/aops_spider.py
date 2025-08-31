import json
import scrapy
from aops_crawler.items import CategoryItem, PostItem
import logging
import re

logger = logging.getLogger(__name__)

class QuotesSpider(scrapy.Spider):
    name = "aops_crawler"

    async def start(self):
        urls = [
            "https://artofproblemsolving.com/community/c13",
        ]
        for url in urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse_contest,
                meta={
                    "driver": "contest",
                    "id": 13,
                    "parent_id": None,
                },
            )

    def parse_contest(self, response):
        # response.body is json.dump.encode('utf-8') we need to decode it to a object   
        json_data = json.loads(response.body.decode('utf-8'))
        # print(json_data)
        # write it to a file
        # with open("response.json", "w") as f:
        #     json.dump(json_data, f)
        for req in json_data["ajax_requests"]:
            rt = req.get("response_json")
            if isinstance(rt, dict):
                # print("--------------------------------")
                cats = (rt.get("response") or {}).get("categories") or []
                logger.info(f"[Spider] Found {len(cats)} categories in contest page")
                for c in cats :
                    if "category_id" in c:
                        yield scrapy.Request(
                            url=f"https://artofproblemsolving.com/community/c{c.get('category_id')}",
                            callback=self.parse_category,
                            meta={
                                "driver": "category",
                                "id": c.get("category_id"),
                                "parent_id": response.meta.get("id", 13),
                            },
                        )
                        # yield scrapy.Request(url=, callback=self.parse_contest,meta={"driver":"contest"})
    def parse_category(self, response):
        # Keep JSON-based logic; skip DOM parsing (to be implemented manually).
        try:
            ctype = (response.headers.get(b"Content-Type") or b"").decode("utf-8", errors="ignore").lower()
        except Exception:
            ctype = ""

        if "application/json" not in ctype:
            # HTML route: select only folder cells with required classes
            elems = response.css('#community-all > div > div.cmty-folder-grid .cmty-category-cell.cmty-category-cell-folder')
            logger.info(f"[Spider] Category {response.meta.get('id')} HTML folder cells: {len(elems)}")
            parent_id = response.meta.get("id")
            for el in elems:
                href = (el.css('a.cmty-full-cell-link::attr(href)').get() or '').strip()
                m = re.search(r'/community/c(\d+)', href)
                if not m:
                    continue
                item_id = int(m.group(1))
                # Extract title and subtitle
                title = (el.xpath('.//div[contains(@class,"cmty-category-cell-title")]/text()[normalize-space()]').get() or '').strip()
                if not title:
                    title = (el.css('.cmty-category-cell-title::text').get() or '').strip()
                subtitle = (el.xpath('.//span[contains(@class,"cmty-category-cell-small-desc")]/text()[normalize-space()]').get() or '').strip()
                if not subtitle:
                    subtitle = (el.css('.cmty-category-cell-desc::text').get() or '').strip()
                if not subtitle:
                    subtitle = (el.css('.cmty-category-cell-long-desc::text').get() or '').strip()

                normalized_url = f"https://artofproblemsolving.com/community/c{item_id}"

                # Recurse to subcategories
                yield scrapy.Request(
                    url=normalized_url,
                    callback=self.parse_category,
                    meta={
                        "driver": "category",
                        "id": item_id,
                        "parent_id": parent_id,
                    }
                )

                # Emit CategoryItem compatible with pipelines
                yield CategoryItem(
                    category_id=item_id,
                    parent_id=parent_id,
                    name=title,
                    url=normalized_url,
                    raw={
                        "item_id": item_id,
                        "item_text": title,
                        "item_subtitle": subtitle or None,
                    },
                )
            return

        try:
            json_data = json.loads(response.body.decode('utf-8'))
        except Exception:
            logger.warning(f"[Spider] Failed to decode JSON for category {response.meta.get('id')}")
            return

        first_filtered = (json_data or {}).get("first_filtered", {})
        response_json = (first_filtered or {}).get("response_json", {})
        category_data = (response_json or {}).get("response", {}).get("category", {})
        items = (category_data or {}).get("items", [])

        logger.info(f"[Spider] Category {response.meta.get('id')} has {len(items)} items (JSON)")

        for item in items:
            item_id = item.get("item_id")
            item_type = item.get("item_type")

            if item_type == "folder" or item_type == 'view_posts':
                yield scrapy.Request(
                    url=f"https://artofproblemsolving.com/community/c{item_id}",
                    callback=self.parse_category,
                    meta={
                        "driver": "category",
                        "id": item_id,
                        "parent_id": response.meta.get("id"),
                    }
                )
                yield CategoryItem(
                    category_id=item_id,
                    parent_id=response.meta.get("id"),
                    name=item.get("title") or item.get("name") or item.get("item_text"),
                    url=f"https://artofproblemsolving.com/community/c{item_id}",
                    raw=item,
                )
            elif item_type == "post" and item.get("post_data", {}).get("post_type") == "forum":
                yield scrapy.Request(
                    url=f"https://artofproblemsolving.com/community/p{item_id}",
                    callback=self.parse_post,
                    meta={
                        "driver": "post",
                        "id": item_id,
                        "parent_id": response.meta.get("id"),
                    }
                )
    def parse_post(self, response):
        yield PostItem(
            post_id=response.meta.get("id"),
            parent_id=response.meta.get("parent_id"),
            url=response.url,
            response=response,
        )
