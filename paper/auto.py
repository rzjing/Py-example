# @Author   : Wang Xiaoqiang
# @GitHub   : https://github.com/rzjing
# @Time     : 2020-07-27 17:45
# @File     : auto.py

from paper.model import Mongo

mogo = Mongo("mongodb://root:s3Ux2agsawjuiO@39.106.42.197:27017/admin")


def autoGroup():
    base = mogo.connection.douban_movies.find_one({'title': '无双'}, {'_id': 0})

    fields = {'title': {'$regex': base['title']}, 'category.name': base['category']['name'], 'contentType': 1}
    for source in ('mgtv', 'youku', 'iqiyi', 'tencent', 'renrenys', 'bilibili'):
        for item in mogo.select(source, 'movie', fields, {'_id': 0, 'id': 1, 'title': 1, 'director': 1, 'actor': 1}):
            # 导演
            base_directors = [d['name'].split()[0] for d in base['director']]
            dest_directors = []
            for d in item['director']:
                if type(d) == str:
                    dest_directors.append(d)
                elif type(d) == dict:
                    dest_directors.append(d['name'])

            # 主演
            base_actors = [a['name'].split()[0] for a in base['actor']]
            dest_actors = []
            for a in item['actor']:
                if type(a) == str:
                    dest_actors.append(a)
                elif type(a) == dict:
                    dest_actors.append(a['name'])

            # 交集
            if set(base_directors + base_actors) & set(dest_directors + dest_actors):
                base.update({'source': {id: item['id'], 'source': source}})
                # db.save(base)
            else:
                print(f'Miss: base -> {base["title"]} dest -> {item["title"]}')


if __name__ == '__main__':
    autoGroup()
