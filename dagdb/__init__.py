import sqlite3
import json
import os


def new():
    return DatabaseClient("dag.sqlite")


def clean():
    os.remove("dag.sqlite")


class DatabaseClient(object):
    def __init__(self, db_path):
        self.dbh = sqlite3.connect(db_path)
        self.dbh.execute("CREATE TABLE IF NOT EXISTS nodes (name TEXT, content TEXT)")

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
