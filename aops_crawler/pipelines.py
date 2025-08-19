# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from aops_crawler.items import CategoryItem, PostItem


class AopsCrawlerPipeline:
    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def open_spider(self, spider):
        print("[PIPELINE] Pipeline opened (no DB writes; logging only)")

    def close_spider(self, spider):
        print("[PIPELINE] Pipeline closed")

    def process_item(self, item, spider):
        if isinstance(item, CategoryItem):
            category_id = item.get("category_id")
            name = item.get("name")
            parent_id = item.get("parent_id")

            print(f"[PIPELINE] CategoryItem: id={category_id}, parent_id={parent_id}, name={name}")
            return item

        if isinstance(item, PostItem):
            post_id = item.get("post_id")
            parent_id = item.get("parent_id")
            source = item.get("url")

            print(f"[PIPELINE] PostItem: id={post_id}, parent_id={parent_id}, source={source}")
            return item

        # default passthrough
        return item
