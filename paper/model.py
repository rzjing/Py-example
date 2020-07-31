# @Author   : Wang Xiaoqiang
# @GitHub   : https://github.com/rzjing
# @Time     : 2020-07-23 16:11
# @File     : model.py

import logging
from datetime import datetime

import pymysql
from pymongo import MongoClient
from pymysql import OperationalError
from pymysql.err import IntegrityError, InternalError, ProgrammingError


class MySQL(object):

    def __init__(self, **kwargs):
        self.params = kwargs
        self.params.update({'charset': 'utf8mb4', "cursorclass": pymysql.cursors.DictCursor})
        try:
            self.connection = pymysql.connect(**self.params)
        except OperationalError as e:
            logging.error(e)

    @property
    def get_params(self):
        return self.params

    def execute(self, sql: str, many=False, long=False):
        """
        :param sql: sql
        :param many: True 返回多值, False 返回单值
        :param long: True 长链接, 需要显示调用 self.commit(), self.close(); False 短链接, 一次调用关闭链接
        :return: str or []
        """
        try:
            self.connection.connect()
            with self.connection.cursor() as cursor:
                try:
                    cursor.execute(sql)
                except (IntegrityError, InternalError, ProgrammingError) as e:
                    logging.error(sql)
                    logging.error(e)
                else:
                    if many:
                        return cursor.fetchall()
                    else:
                        return cursor.fetchone()
        except OperationalError as e:
            logging.error(e)
        finally:
            if not long:
                self.commit()
                self.close()

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()


class Mongo(object):

    def __init__(self, conf):
        self.params = conf
        self.connection = MongoClient(conf)["jinwu"]
        self.table = {
            "ft": {
                "token": self.connection.ft_tokens
            },
            "douban": {
                "movie": self.connection.douban_movies,
                "person": self.connection.douban_persons
            },
            "tencent": {
                "movie": self.connection.tencent_movies,
                "episode": self.connection.tencent_episode,
                "movieTv": self.connection.tencent_movies_Tv
            },
            "iqiyi": {
                "channel": self.connection.iqiyi_channel,
                "movie": self.connection.iqiyi_movies,
                "episode": self.connection.iqiyi_episode,
                "person": self.connection.iqiyi_persons
                # "trick": self.connection.iqiyi_tricks
            },
            "bilibili": {
                "movie": self.connection.bilibili_movies,
                "episode": self.connection.bilibili_episode
            },
            "youku": {
                "movie": self.connection.youku_movies,
                "episode": self.connection.youku_episode
            },
            "renrenys": {
                "movie": self.connection.renrenys_movies
            },
            "mgtv": {
                "movie": self.connection.mgtv_movies,
                "episode": self.connection.mgtv_episode
            },
            "content": {
                "dangbei": self.connection.content_dangbei
            },
        }

    def select(self, source, _type, fields: dict, filters=None):
        if filters is None:
            filters = {'_id': 0}
        table = self.table.get(source).get(_type)
        return table.find(fields, filters)

    def insert(self, source, _type, data: dict):
        # state = 1 新入库, created = 创建、更新时间
        data.update(
            {"state": 1, "created": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        )
        self.table.get(source).get(_type).insert_one(data)

    def update(self, source, _type, data: dict):
        table = self.table.get(source).get(_type)
        r = table.update_one({"id": data["id"]}, {"$set": data})
        # 数据不存在则插入
        if not r.matched_count:
            self.insert(source, _type, data)
        # 数据更新则变更状态
        elif r.modified_count:
            table.update_one(
                {"id": data["id"]},
                {"$set": {"state": 1, "created": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
            )


if __name__ == '__main__':
    pass
