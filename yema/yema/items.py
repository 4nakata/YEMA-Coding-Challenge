# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Product(scrapy.Item):
    upc_gtin = scrapy.Field()
    brand = scrapy.Field()
    name = scrapy.Field()
    description = scrapy.Field()
    ingredients = scrapy.Field()
    package = scrapy.Field()


class Branch(scrapy.Item):
    product_id = scrapy.Field()
    chain = scrapy.Field()
    branch = scrapy.Field()
    price = scrapy.Field()
    category = scrapy.Field()
    product_url = scrapy.Field()
