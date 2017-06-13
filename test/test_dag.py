import dagdb
import pytest
import json


class JsonRestClient(object):
    def __init__(self, api_client):
        self.api_client = api_client

    def get(self, url):
        rv = self.api_client.get(url)
        return JsonRestResponse(rv)

    def post(self, url, data):
        rv = self.api_client.post(
            url,
            data=json.dumps(data),
            content_type='application/json')
        return JsonRestResponse(rv)


class JsonRestResponse(object):
    def __init__(self, api_response):
        self.api_response = api_response

    @property
    def status(self):
        return self.api_response.status

    @property
    def data(self):
        raw_json = self.api_response.get_data(as_text=True)
        document = json.loads(raw_json)
        return document['data']


@pytest.fixture
def api_client():
    dagdb.app.config['TESTING'] = True
    return JsonRestClient(dagdb.app.test_client())


@pytest.fixture
def db():
    dagdb.clean()
    return dagdb.new()


def test_can_dag():
    return True


def test_zero_nodes_in_empty_db(db):
    assert db.num_nodes == 0


@pytest.fixture
def junk_node():
    return dict(poop=True)


def test_can_insert_node(db, junk_node):
    db.insert('junk', junk_node)


def test_insert_increases_node_count(db, junk_node):
    db.insert('junk', junk_node)
    assert db.num_nodes == 1


def test_can_find_node(db, junk_node):
    db.insert('junk', junk_node)
    nodes = db.find_all()
    assert nodes[0] == junk_node


def test_can_persist_nodes_between_sessions(junk_node):
    first_session = dagdb.new()
    first_session.insert('junk', junk_node)

    second_session = dagdb.new()
    nodes = second_session.find_all()
    assert nodes[0] == junk_node


def test_retrieve_node_by_id(db, junk_node):
    db.insert('junk', junk_node)

    node = db.find('junk')
    assert node['poop'] == True


@pytest.fixture
def db_with_three_nodes(db):
    db.insert('one', dict(value=1))
    db.insert('two', dict(value=2))
    db.insert('three', dict(value=3))
    return db


def test_can_link_nodes(db_with_three_nodes):
    db_with_three_nodes.link('one', 'two')
    one = db_with_three_nodes.find('one')
    two = db_with_three_nodes.get_referants('one')[0]
    assert two['value'] == 2


def test_can_link_multiple_nodes(db_with_three_nodes):
    db_with_three_nodes.link('one', 'two')
    db_with_three_nodes.link('one', 'three')
    one = db_with_three_nodes.find('one')
    assert len(db_with_three_nodes.get_referants('one')) == 2


def test_can_call_api(api_client):
    rv = api_client.get("/nodes")
    assert rv.status == '200 OK'


def test_find_nodes_with_api(db_with_three_nodes, api_client):
    db_with_three_nodes.insert('one', dict(value=1))
    rv = api_client.get("/nodes")
    nodes = rv.data
    assert nodes[0]['value'] == 1 


def test_find_three_nodes_with_api(db_with_three_nodes, api_client):
    rv = api_client.get("/nodes")
    nodes = rv.data
    assert len(nodes) == 3


def test_find_referants_of_node(db_with_three_nodes, api_client):
    db_with_three_nodes.link('one', 'two')
    db_with_three_nodes.link('one', 'three')
    rv = api_client.get("/nodes/one/links")
    referants = rv.data
    assert referants[1]['value'] == 3


def test_add_referant_to_node(db_with_three_nodes, api_client):
    rv = api_client.get("/nodes/one/links")
    referants = rv.data
    assert len(referants) == 0

    rv = api_client.post("/nodes/one/links", data=dict(links=['two']))
    referants = rv.data
    assert len(referants) == 1


def test_add_referants_to_node(db_with_three_nodes, api_client):
    rv = api_client.post("/nodes/one/links", data=dict(links=['two', 'three']))
    referants = rv.data
    assert referants[1]['value'] == 3
