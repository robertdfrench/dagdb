import dagdb
import pytest
import json


@pytest.fixture
def db():
    yield dagdb.new()
    dagdb.clean()


def test_can_dag():
    return True


@pytest.fixture
def junk_node():
    return dict(name='earl')


def test_can_insert_node(db, junk_node):
    db.insert('junk', junk_node)


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
    assert node['name'] == 'earl'


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
