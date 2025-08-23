# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AopsCrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class CategoryItem(scrapy.Item):
    category_id = scrapy.Field()
    parent_id = scrapy.Field()
    name = scrapy.Field()
    url = scrapy.Field()
    raw = scrapy.Field()


class PostItem(scrapy.Item):
    post_id = scrapy.Field()
    parent_id = scrapy.Field()
    url = scrapy.Field()
    response = scrapy.Field()  # carry the full Scrapy response for pipeline parsing