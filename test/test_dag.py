import dagdb
import pytest


@pytest.fixture
def api_client():
    dagdb.app.config['TESTING'] = True
    return dagdb.app.test_client()


@pytest.fixture
def db():
    dagdb.clean()
    return dagdb.new()


def test_can_dag():
    return True


def test_zero_nodes_in_empty_db(db):
    assert db.num_nodes == 0


def test_can_insert_node(db):
    node = dict(poop=True)
    db.insert('junk', node)


def test_insert_increases_node_count(db):
    node = dict(poop=True)
    db.insert('junk', node)
    assert db.num_nodes == 1


def test_can_find_node(db):
    node = dict(poop=True)
    db.insert('junk', node)
    nodes = db.find_all()
    assert nodes[0] == node

def test_can_persist_nodes_between_sessions():
    first_session = dagdb.new()
    node = dict(poop=True)
    first_session.insert('junk', node)

    second_session = dagdb.new()
    nodes = second_session.find_all()
    assert nodes[0] == node


def test_retrieve_node_by_id(db):
    db.insert('shitline', dict(happy='roo'))

    node = db.find('shitline')
    assert node['happy'] == 'roo'


def test_can_link_nodes(db):
    db.insert('one', dict(value=1))
    db.insert('two', dict(value=2))
    db.link('one', 'two')
    one = db.find('one')
    two = db.get_referants('one')[0]
    assert two['value'] == 2


def test_can_link_multiple_nodes(db):
    db.insert('one', dict(value=1))
    db.insert('two', dict(value=2))
    db.insert('three', dict(value=3))
    db.link('one', 'two')
    db.link('one', 'three')
    one = db.find('one')
    assert len(db.get_referants('one')) == 2


def test_can_call_api(api_client):
    rv = api_client.get("/nodes")
    assert rv.status == '200 OK'
