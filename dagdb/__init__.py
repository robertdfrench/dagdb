from dagdb import dynamodb
import json


driver = dynamodb


def new():
    return DatabaseClient()


def clean():
    return driver.clean()


class DatabaseClient(object):
    def __init__(self):
        self.table = driver.new()

    @property
    def num_nodes(self):
        return self.table.num_nodes()

    def insert(self, name, node):
        self.table.insert(name, json.dumps(node))

    def find_all(self):
        return [json.loads(row) for row in self.table.find_all()]

    def find(self, name):
        return json.loads(self.table.find(name))

    def link(self, a, b):
        self.table.link(a, b)

    def get_referants(self, name):
        return [json.loads(node) for node in self.table.get_referants(name)]
