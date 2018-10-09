# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class UserItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    answer_count = scrapy.Field()
    articles_count = scrapy.Field()
    avatar_url = scrapy.Field()
    badge = scrapy.Field()
    follower_coun = scrapy.Field()
    gender = scrapy.Field()
    headline = scrapy.Field()
    id = scrapy.Field()
    is_advertise = scrapy.Field()
    is_followed = scrapy.Field()
    is_following = scrapy.Field()
    is_org = scrapy.Field()
    name = scrapy.Field()
    type = scrapy.Field()
    url = scrapy.Field()
    url_token = scrapy.Field()
    user_type = scrapy.Field()





