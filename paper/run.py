# @Author   : Wang Xiaoqiang
# @GitHub   : https://github.com/rzjing
# @Time     : 2020-07-23 15:36
# @File     : run.py

from elasticsearch import Elasticsearch
from flask import Flask, jsonify, request
from flask_restful import Api, Resource, reqparse

from paper.model import MySQL

app = Flask(__name__)
v1 = Api(app, prefix='/v1')
db = MySQL(host='127.0.0.1', port=3306, user='root', password='123456', database='jinwu')
parser = reqparse.RequestParser()


def make_response(code=200, **kwargs):
    return jsonify(code=code, **kwargs)


@app.route('/v1/roles/', methods=['GET'])
def getRoles(name=None):
    r = db.execute(
        f"select id, rolename as name, unix_timestamp(created_at) as created_at, unix_timestamp(updated_at) as updated_at from jw_account_role order by updated_at desc;",
        many=True)
    return make_response(data=r)


rolesParams = parser.copy()
rolesParams.add_argument('name', type=str, location=['args', 'form', 'json'], required=True)


class Roles(Resource):
    def __init__(self):
        self.args = rolesParams.parse_args()

    def post(self):
        r = db.execute(f"select id from jw_account_role where rolename = '{self.args['name']}';")
        if r:
            return make_response(code=400, error='角色已存在')
        else:
            db.execute(f"insert into jw_account_role set rolename = '{self.args['name']}';")
        return make_response(code=201, msg='created')


@app.route('/v1/accounts/', methods=['GET'])
def getAccounts():
    r = db.execute(
        f"select a.id, a.username, a.state, r.rolename, unix_timestamp(a.created_at) as created_at, unix_timestamp(a.updated_at) as updated_at from jw_account as a left join jw_account_role as r on a.role_id = r.id order by updated_at desc;",
        many=True)
    return make_response(data=r)


v1.add_resource(Roles, '/roles/')

accountsParams = parser.copy()
accountsParams.add_argument('username', type=str, location=['args', 'form', 'json'], required=True)
accountsParams.add_argument('password', type=str, location=['args', 'form', 'json'], required=True)
accountsParams.add_argument('state', type=int, location=['args', 'form', 'json'], required=True)
accountsParams.add_argument('role_id', type=int, location=['args', 'form', 'json'], required=True)


class Accounts(Resource):
    def __init__(self):
        self.args = accountsParams.parse_args()

    def post(self):
        r = db.execute(f"select id from jw_account where username = '{self.args['username']}';")
        if r:
            return make_response(code=400, error='账号已存在')
        else:
            db.execute(
                f"insert into jw_account (username, password, state, role_id) value ('{self.args['username']}','{self.args['password']}','{self.args['state']}','{self.args['role_id']}');")
        return make_response(code=201, msg='created')


v1.add_resource(Accounts, '/accounts/')

roleParams = parser.copy()


class Role(Resource):
    def __init__(self):
        self.args = roleParams.parse_args()

    @staticmethod
    def get(rid):
        r = db.execute(
            f"select id, name, unix_timestamp(created_at) as created_at, unix_timestamp(updated_at) as updated_at from jw_account_role where id = {rid};"
        )
        if r:
            return make_response(data=r)
        return make_response(code=400, error='角色不存在')

    def post(self, rid):
        r = db.execute(f"select id from jw_account_role where id = {rid};")
        if r:
            return make_response(code=400, error='角色已存在')
        else:
            try:
                db.execute(f"insert into jw_account_role (name) value ({self.args['name']});")
            except Exception:
                return make_response(code=400, error='创建失败')

        return make_response(code=201, msg='created')

    def put(self, rid):
        try:
            db.execute(f"update jw_account_role set name = {self.args['name']} where id = {rid};")
        except Exception:
            return make_response(code=400, error='更新失败')

        return make_response(msg='changed')

    @staticmethod
    def delete(rid):
        db.execute(f"delete from jw_account_role where id = {rid};")
        return make_response(msg='deleted')


v1.add_resource(Role, '/v1/role/<int:rid>')

accountParams = parser.copy()
accountParams.add_argument('username', type=str, location=['args', 'form', 'json'])
accountParams.add_argument('password', type=str, location=['args', 'form', 'json'])
accountParams.add_argument('state', type=str, location=['args', 'form', 'json'])
accountParams.add_argument('role_id', type=int, location=['args', 'form', 'json'])


class Account(Resource):
    def __init__(self):
        self.args = accountParams.parse_args()

    @staticmethod
    def get(aid):
        r = db.execute(
            f"select a.id, a.username, a.state, r.rolename, unix_timestamp(a.created_at) as created_at, unix_timestamp(a.updated_at) as updated_at from jw_account as a left join jw_account_role as r on a.role_id = r.id where a.id = {aid};"
        )
        if r:
            return make_response(data=r)
        return make_response(code=400, error='账号不存在')

    def post(self, aid):
        r = db.execute(f"select id from jw_account where id = {aid};")
        if r:
            return make_response(code=400, error='账号已存在')
        else:
            try:
                db.execute(
                    f"insert into jw_account (username,password,state,role_id) value ({self.args['username']},{self.args['password']},{self.args['state']},{self.args['role_id']});")
            except Exception:
                return make_response(code=400, error='创建失败')

        return make_response(code=201, msg='created')

    def put(self, aid):
        r = db.execute(f"select id from jw_account where id = {aid};")
        if r:
            query = [
                f"{k} = {v}" for k, v in self.args.items() if v
            ]
            if query:
                try:
                    db.execute(f"update jw_account set {','.join(query)} where id = {aid};")
                except Exception:
                    return make_response(code=400, error='更新失败')
                else:
                    return make_response(msg='changed')
        return make_response(code=400, error='账号不存在')

    @staticmethod
    def delete(aid):
        db.execute(f"delete from jw_account where id = {aid};")
        return make_response(msg='deleted')


v1.add_resource(Account, '/account/<int:aid>')

resourceParams = parser.copy()
resourceParams.add_argument('title', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('title_other', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('description', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('cover', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('alias', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('score', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('views', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('genre', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('area', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('tags', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('language', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('duration', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('year', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('isPay', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('isPays', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('contentType', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('releaseDate', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('onLines', type=str, location=['args', 'form', 'json'], required=True)
resourceParams.add_argument('status', type=str, location=['args', 'form', 'json'], required=True)


class Resource(Resource):
    def __init__(self):
        self.args = resourceParams.parse_args()

    def put(self, rid):
        sql = f"replace into jw_resource (%s) value (%s) where id = {rid};" % (
            ','.join(list(self.args.keys())), ','.join(["'%s'" % item for item in list(self.args.values())])
        )
        db.execute(sql)
        return make_response(msg='changed')


v1.add_resource(Resource, '/resource/<int:rid>')

es = Elasticsearch(['123.57.67.240:9200'])


@app.route('/v1/search', methods=['GET'])
def search(rid=None):
    rid = request.args.get('id')
    title = request.args.get('title')
    page = int(request.args.get('page', 0))
    size = request.args.get('page_size', 20)

    if page: page -= 1

    index = 'ai_spider_album'
    if rid:
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "id": rid
                            }
                        }
                    ]
                }
            },
            "from": page,
            "size": size
        }
    elif title:
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "title": title
                            }
                        }
                    ]
                }
            },
            "from": page,
            "size": size
        }
    else:
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_all": {}
                        }
                    ]
                }
            },
            "from": page,
            "size": size,
        }

    result = es.search(index=index, body=body)
    return make_response(data=[item['_source'] for item in result['hits']['hits']])


@app.route('/login', methods=['POST'])
def login():
    pass


@app.route('/logout', methods=['POST'])
def logout():
    pass


if __name__ == '__main__':
    app.run('0.0.0.0', port=5100, debug=True)
