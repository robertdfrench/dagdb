import sqlite3
import os


def new():
    return SqliteTable("dag.sqlite")


def clean():
    if os.path.exists("dag.sqlite"):
        os.remove("dag.sqlite")


class SqliteTable(object):
    def __init__(self, db_path):
        self.db_path = db_path
        self.dbh = sqlite3.connect(db_path)
        self.migrate()

    def migrate(self):
        self.dbh.execute("CREATE TABLE IF NOT EXISTS nodes (name TEXT, content TEXT)")
        self.dbh.execute("CREATE TABLE IF NOT EXISTS edges (source TEXT, dest TEXT)")

    @property
    def num_nodes(self):
        rv = self.dbh.execute("SELECT COUNT(*) FROM nodes")
        return rv.fetchone()[0]

    def insert(self, name, content):
        self.dbh.execute("INSERT INTO nodes (name, content) VALUES ('%s', '%s')" % (name, content))
        self.dbh.commit()

    def find_all(self):
        rv = self.dbh.execute("SELECT * FROM nodes")
        return [row[1] for row in rv.fetchall()]
    
    def find(self, name): 
        rv = self.dbh.execute("SELECT * FROM nodes WHERE name='%s'" % name)
        row = rv.fetchone()
        return row[1]

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
                refs.append(node[1])
        return refs
