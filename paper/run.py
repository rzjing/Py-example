# @Author   : Wang Xiaoqiang
# @GitHub   : https://github.com/rzjing
# @Time     : 2020-07-23 15:36
# @File     : run.py

from flask import Flask, jsonify
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
            f"select a.id, a.username, a.state, r.rolename, unix_timestamp(a.created_at) as created_at, unix_timestamp(a.updated_at) as updated_at from jw_account as a left join jw_account_role as r on a.role_id = r.id where a.id = {aid};")

        if r:
            return make_response(data=r)
        return make_response(code=400, error='账号不存在')

    def put(self, aid):
        r = db.execute(f"select id from jw_account where id = {aid};")
        if r:
            query = ""
            if self.args['password']:
                query += f"set password = '{self.args['password']}'"
            if self.args['state']:
                query += f", state = '{self.args['state']}'"
            if self.args['role_id']:
                query += f", role_id = '{self.args['role_id']}'"

            if query:
                db.execute(f"update jw_account {query} where id = {aid};")
            return make_response(msg='changed')
        return make_response(code=400, error='账号不存在')

    @staticmethod
    def delete(aid):
        db.execute(f"delete from jw_account where id = {aid};")
        return make_response(msg='deleted')


v1.add_resource(Account, '/account/<int:aid>')

if __name__ == '__main__':
    app.run('0.0.0.0', port=5100, debug=True)
