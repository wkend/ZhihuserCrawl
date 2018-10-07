# -*- coding: utf-8 -*-

import json
from scrapy import Spider, Request
from zhihuuser.items import UserItem


class ZhihuSpider(Spider):
    name = 'zhihu'  # 用于区别Spider,唯一
    allowed_domains = ['www.zhihu.com'] # 允许爬取的域名列表
    start_urls = ['http://www.zhihu.com/']  # Spider在启动时进行爬取的url列表

    # 指定一个开始的用户，
    start_user = 'excited-vczh'

    # 某个用户的详情
    user_url = 'https://www.zhihu.com/api/v4/members/{user}?include={include}'
    user_query = 'allow_message,is_followed,is_following,is_org,is_blocking,employments,answer_count,follower_count,articles_count,gender,badge[?(type=best_answerer)].topics'

    # 起始用户关注的用户
    followees_url = 'https://www.zhihu.com/api/v4/members/{user}/followees?include={include}&offset={offset}&limit={limit}'
    followees_query = 'data[*].answer_count,articles_count,gender,follower_count,is_followed,is_following,badge[?(type=best_answerer)].topics'

    # 起始用户的粉丝
    followers_url = 'https://www.zhihu.com/api/v4/members/{user}/followers?include={include}&offset={offset}&limit={limit}'
    followers_query = 'data[*].answer_count,articles_count,gender,follower_count,is_followed,is_following,badge[?(type=best_answerer)].topics'

    def start_requests(self):
        '''
        一个生成器，仅仅被spider调用一次，用于第一个Request
        :return: 一个迭代器对象
        '''
        # 向起始用户发起请求
        yield Request(self.user_url.format(user=self.start_user, include=self.user_query), callback=self.parse_user)
        # 爬取起始用户关注的用户
        yield Request(self.followees_url.format(user=self.start_user, include=self.followees_query, offset=0, limit=20),
                      callback=self.parse_followees)
        # 爬取起始用户的粉丝用户
        yield Request(self.followers_url.format(user=self.start_user, include=self.followers_query, offset=0, limit=20),
                      callback=self.parse_followers)

    def parse_user(self, response):
        '''
        解析用户详情信息;获取某个用户的关注列表;获取某个用户的粉丝列表
        :param response:
        :return:
        '''
        result = json.loads(response.text)  # 将请求得到的json字符串转换为字典
        # print(result)
        item = UserItem()
        for field in item.fields:
            if field in result.keys():
                item[field] = result.get(field)
        yield item

        # 请求每个用户的关注列表
        yield Request(
            self.followees_url.format(user=result.get('url_token'), include=self.followees_query, offset=0, limit=20),
            callback=self.parse_followees)
        # 请求每个用户的粉丝列表
        yield Request(
            self.followers_url.format(user=result.get('url_token'), include=self.followers_query, offset=0, limit=20),
            callback=self.parse_followers)

    def parse_followees(self, response):
        '''
        解析起始用户关注的用户
        :param response:
        :return:
        '''
        results = json.loads(response.text)
        if 'data' in results.keys():
            for result in results.get('data'):
                yield Request(self.user_url.format(user=result.get('url_token'), include=self.user_query),
                              callback=self.parse_user)
        if 'paging' in results.keys() and results.get('paging').get('is_end') == False:
            next_page = results.get('paging').get('next')
            yield Request(next_page, callback=self.parse_followees)

    def parse_followers(self, response):
        '''
        解析起始用户的粉丝用户
        :param response:
        :return:
        '''
        results = json.loads(response.text)
        if 'data' in results.keys():
            for result in results.get('data'):
                yield Request(self.user_url.format(user=result.get('url_token'), include=self.user_query),
                              callback=self.parse_user)
        if 'paging' in results.keys() and results.get('paging').get('is_end') == False:
            next_page = results.get('paging').get('next')
            yield Request(next_page, callback=self.parse_followers)
