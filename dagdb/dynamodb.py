from pynamodb.models import Model
from pynamodb.connection import Connection
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
from pynamodb.attributes import UnicodeAttribute
import uuid


def new():
    return DynamoEngine()


def clean():
    for item in Node.scan():
        item.delete()
    for item in Edge.scan():
        item.delete()


class DynamoEngine(object):
    def __init__(self):
        self.migrate()

    def migrate(self):
        Node.create_table(read_capacity_units=1, write_capacity_units=1)
        Edge.create_table(read_capacity_units=1, write_capacity_units=1)

    def insert(self, name, content):
        node = Node(name, content=content)
        node.save()

    def find_all(self):
        return [node.content for node in Node.scan()]
    
    def find(self, name): 
        return Node.first(name).content

    def link(self, a, b):
        edge_id = "%s" % uuid.uuid4()
        edge = Edge(edge_id, source=a, destination=b)
        edge.save()

    def get_referants(self, name):
        refs = list()
        for edge in EdgeSourceView.query(name):
            node = Node.first(edge.destination)
            if node:
                refs.append(node.content)
        return refs


class GetFirst(object):
    @classmethod
    def first(cls, param):
        items = list(cls.query(param, limit=1))
        if items:
            return items[0]


class Node(Model, GetFirst):
    class Meta:
        table_name = "Nodes"
    name = UnicodeAttribute(hash_key=True)
    content = UnicodeAttribute()


class EdgeSourceView(GlobalSecondaryIndex):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()
    source = UnicodeAttribute(hash_key=True)


class EdgeDestinationView(GlobalSecondaryIndex):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()
    destination = UnicodeAttribute(hash_key=True)


class Edge(Model, GetFirst):
    class Meta:
        table_name = "Edges"
    name = UnicodeAttribute(hash_key=True)
    source = UnicodeAttribute()
    source_index = EdgeSourceView()
    destination = UnicodeAttribute()
    destination_index = EdgeDestinationView()
