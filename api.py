import pymysql
import pymysql.cursors
import json
from flask import Flask
from flask import request

api = Flask(__name__)

path = "/api/v{version}".format(**{'version': 1.0})

config = {
    'mysql':
        {'user': 'cloud',
         'password': 'L3ir142019()',
         'database': 'meicm-1819-cn',
         'hostname': '52.15.129.99',
         'port': 3306,
         },
    'logging dir': '/tmp/'
}


def _valid_json_fields(dictionary: dict, fields: list) -> bool:
    return all(ff in dictionary.keys() for ff in fields)


@api.errorhandler(AssertionError)
def _err_assert(_failure):
    return "Invalid Data", 400


@api.errorhandler(ValueError)
def _err_value(failure):
    return str(failure), 400


@api.errorhandler(404)
def _err_not_found(_failure):
    return 'not found', 404
    # return render_template('page_not_found.html'), 404


@api.route(path + "/")
def index():
    return "<h1 style='color:blue'>METADATA API!</h1>"


@api.route(path + '/status', methods=['GET'])
def api_status():
    return 'running!'


@api.route(path + '/status', methods=['GET'])
def api_status_clean():
    return 'running!'

def _api_getmysqlconn():
    domain = config['mysql']['hostname']
    username = config['mysql']['user']
    password = config['mysql']['password']
    database = config['mysql']['database']
    port = config['mysql']['port']
    return pymysql.connect(host=domain, user=username, password=password,
                           db=database, port=port, ssl=None,
                           cursorclass=pymysql.cursors.DictCursor,
                           charset='utf8mb4', autocommit=True)


@api.route(path + '/mysql', methods=['GET'])
def api_mysql_stat():
    conn = _api_getmysqlconn()
    with conn.cursor() as cursor:
        cursor.execute('SELECT 1 as `FIRST VALUE`, 2 as `SECOND VALUE`, UNIX_TIMESTAMP() AS `TIME`')
        read_data = cursor.fetchall()

    conn.close()
    return json.dumps(read_data)


@api.route(path + "/voter/byname/<string:name>", methods=['GET'])
def voter_byname(name: str):
    query = "SELECT * FROM voters WHERE name LIKE %(voter name)s"
    params = {'voter name': name}

    dataset = {'meta': {'handled_id': request.host}}

    conn = _api_getmysqlconn()
    with conn.cursor() as cursor:
        records = cursor.execute(query, params)
        dataset['meta']['records_on_data'] = records

        if records:
            dataset['result'] = cursor.fetchall()
        else:
            dataset['msg'] = 'not found'

    conn.close()

    return json.dumps(dataset, separators=(',', ':'))


@api.route(path + "/voter/bysection/<int:section>/<int:limit>", methods=['GET'])
def voter_bysection(section: int, limit: int):
    query = "SELECT * FROM voters WHERE section = %(section)s LIMIT %(limit)s"
    params = {'section': section, 'limit': limit}

    dataset = {'meta': {'handled_id': request.host}}

    conn = _api_getmysqlconn()
    with conn.cursor() as cursor:
        records = cursor.execute(query, params)
        dataset['meta']['records_on_data'] = records

        if records:
            dataset['result'] = cursor.fetchall()[0]
        else:
            dataset['msg'] = 'not found'

    conn.close()

    return json.dumps(dataset, separators=(',', ':'))


@api.route(path + "/sections", methods=['GET'])
def voter_sections():
    query = "SELECT count(id) AS `total`, section FROM voters GROUP BY section"

    dataset = {'meta': {'handled_id': request.host}}

    conn = _api_getmysqlconn()
    with conn.cursor() as cursor:
        records = cursor.execute(query)
        dataset['meta']['records_on_data'] = records
        dataset['result'] = cursor.fetchall()

    conn.close()

    return json.dumps(dataset, separators=(',', ':'))


@api.route(path + "/voters/<int:voter_id>/vote", methods=['PATCH'])
def voters_setvote(voter_id: int):
    query_update = "UPDATE voters SET has_voted = 1 WHERE id = %(voter id)s AND has_voted = 0"
    query_select = "SELECT * FROM voters WHERE id = %(voter id)s"

    param = {'voter id': voter_id}

    dataset = {'meta': {'handled_id': request.host}}

    conn = _api_getmysqlconn()
    with conn.cursor() as cursor:
        records = cursor.execute(query_update, *[param])
        dataset['meta']['records_on_data'] = records

        if records:
            cursor.execute(query_select, *[param])
            dataset['result'] = cursor.fetchall()[0]
        else:
            dataset['msg'] = 'Voter has already voted'

    conn.close()

    if records:
        return json.dumps(dataset, separators=(',', ':'))
    else:
        return json.dumps(dataset, separators=(',', ':')), 409


@api.route(path + "/voters/reset", methods=['PATCH'])
def voters_reset():
    query_update = "UPDATE voters SET has_voted = 0"

    dataset = {'meta': {'handled_id': request.host}}

    conn = _api_getmysqlconn()
    with conn.cursor() as cursor:
        records = cursor.execute(query_update)
        dataset['meta']['records_on_data'] = records

    conn.close()

    return json.dumps(dataset, separators=(',', ':'))


@api.route(path + "/voter/byvoter/<int:voterid>", methods=['GET'])
def voter_byvoter(voterid: int):
    query = "SELECT * FROM voters WHERE voter_number = %(voter number)s"

    params = {'voter number': voterid}

    dataset = {'meta': {'handled_id': request.host}}

    conn = _api_getmysqlconn()
    with conn.cursor() as cursor:
        records = cursor.execute(query, params)
        dataset['meta']['records_on_data'] = records

        if records:
            dataset['result'] = cursor.fetchall()[0]
        else:
            dataset['msg'] = 'not found'

    conn.close()

    return json.dumps(dataset, separators=(',', ':'))


@api.route(path + "/voter/byid/<int:uid>", methods=['GET'])
def voter_byid(uid: int):
    query = "SELECT * FROM voters WHERE id = %(id)s"

    params = {'id': uid}

    dataset = {'meta': {'handled_id': request.host}}

    conn = _api_getmysqlconn()
    with conn.cursor() as cursor:
        records = cursor.execute(query, params)
        dataset['meta']['records_on_data'] = records

        if records:
            dataset['result'] = cursor.fetchall()[0]
        else:
            dataset['msg'] = 'not found'

    conn.close()

    return json.dumps(dataset, separators=(',', ':'))


if __name__ == "__main__":
    api.run()
