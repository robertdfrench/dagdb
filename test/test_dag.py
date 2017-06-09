import dagdb
import pytest


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
    db.insert(node)


def test_insert_increases_node_count(db):
    node = dict(poop=True)
    db.insert(node)
    assert db.num_nodes == 1


def test_can_find_node(db):
    node = dict(poop=True)
    db.insert(node)
    nodes = db.find_all()
    assert nodes[0] == node

def test_can_persist_nodes_between_sessions():
    first_session = dagdb.new()
    node = dict(poop=True)
    first_session.insert(node)

    second_session = dagdb.new()
    nodes = second_session.find_all()
    assert nodes[0] == node
