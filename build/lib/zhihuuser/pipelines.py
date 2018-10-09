# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo

"""
当Item在Spider中被收集之后，它将会被传递到Item Pipeline，一些组件会按照一定的顺序执行对Item的处理;
这里将爬取结果存储在数据库中。
"""
class MongoPipeline(object):
    collection_name = 'scrapy_items'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        '''
        从配置文件中获取所需要的配置信息并返回
        :param crawler:
        :return:
        '''
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        '''
        当spider被开启时该方法被调用
        :param spider: 被开启的spider
        :return:
        '''
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        '''
        当spider被关闭时，这个方法被调用
        :param spider: 被关闭的spider
        :return:
        '''
        self.client.close()

    def process_item(self, item, spider):
        '''
        每个item pipeline组件都需要调用该方法
        :param item:被爬取的item
        :param spider:爬取该item的spider
        :return:返回一个具有数据的dict，或是 Item (或任何继承类)对象， 或是抛出 DropItem 异常，被丢弃的item将不会被之后的pipeline组件所处理。
        '''
        # 利用update实现去重
        self.db['user'].update({'url_token':item['url_token']},{'$set':item},True)
        return item
