import sqlite3
import json
import os
import flask


app = flask.Flask('dagdb')


def new():
    return DatabaseClient("dag.sqlite")


def clean():
    if os.path.exists("dag.sqlite"):
        os.remove("dag.sqlite")


class DatabaseClient(object):
    def __init__(self, db_path):
        self.dbh = sqlite3.connect(db_path)
        self.dbh.execute("CREATE TABLE IF NOT EXISTS nodes (name TEXT, content TEXT)")
        self.dbh.execute("CREATE TABLE IF NOT EXISTS edges (source TEXT, dest TEXT)")

    @property
    def num_nodes(self):
        rv = self.dbh.execute("SELECT COUNT(*) FROM nodes")
        return rv.fetchone()[0]

    def insert(self, name, node):
        self.dbh.execute("INSERT INTO nodes (name, content) VALUES ('%s', '%s')" % (name, json.dumps(node)))
        self.dbh.commit()

    def find_all(self):
        rv = self.dbh.execute("SELECT * FROM nodes")
        return [json.loads(row[1]) for row in rv.fetchall()]

    def find(self, name):
        rv = self.dbh.execute("SELECT * FROM nodes WHERE name='%s'" % name)
        row = rv.fetchone()
        return json.loads(row[1])

    def link(self, a, b):
        self.dbh.execute("INSERT INTO edges (source, dest) values ('%s', '%s')" % (a,b))
        self.dbh.commit()

    def get_referants(self, name):
        rv = self.dbh.execute("SELECT * FROM edges WHERE source='%s'" % name)
        refs = list()
        for row in rv.fetchall():
            rv2 = self.dbh.execute("SELECT * FROM nodes WHERE name='%s'" % row[1])
            node = rv2.fetchone()
            if node:
                refs.append(json.loads(node[1]))
        return refs


@app.route("/nodes")
def nodes():
    nodes = new().find_all()
    response = dict(data=nodes)
    return flask.jsonify(response)


@app.route("/nodes/<name>/links")
def node_links(name):
    nodes = new().get_referants(name)
    response = dict(data=nodes)
    return flask.jsonify(response)


@app.route("/nodes/<name>/links", methods=['POST'])
def add_node_links(name):
    db = new()
    link_plan = flask.request.get_json()
    for destination in link_plan['links']:
        db.link(name, destination)
    nodes = db.get_referants(name)
    response = dict(data=nodes)
    return flask.jsonify(response)
