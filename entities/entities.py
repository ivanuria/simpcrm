# Entities files

from databases.databases import DBInterface
from databases.sqlite import SqliteInterface
from collections import defaultdict
from .fields import Field, Fields

class Item(dict):
    persistent = defaultdict(dict)

    def __new__(cls, entity, data={}):
        if entity in cls.persistent:
            pass


class Entity:
    persistent = defaultdict(dict)
    # A dictionary with an entity by database. Why? Suddenly my intuition sais I must do this

    def __new__(cls, database, table, name, fields, description, parent="", parent_field=""):
        if table in cls.persistent[database]:
            self = cls.persistent[database][table]
            if any([name!=self.name,
                   isinstance(fields, Fields) and fields!=self.fields,
                   not isinstance(fields, Fields) and fields!=dict(zip(self.fields.keys(), self.fields.values())),
                   parent!=self.parent,
                   parent_field!=self.parent_field]):
                print(name)
                print(fields)
                print(self.fields)
                print(parent, self.parent)
                raise Exception("Entity already defined")
            else:
                return self
        else:
            return super().__new__(cls)

    def __init__(self, database, table, name, fields, description, parent="", parent_field=""):
        assert isinstance(database, DBInterface)
        assert isinstance(fields, dict)
        self.persistent[database][table] = self
        self._name = name
        self._table = table
        self.description = description
        if isinstance(fields, Fields):
            self._fields = fields
        else:
            self._fields = Fields(database, table, fields)
        self._database = database
        self._parent = parent
        if isinstance(parent, Entity):
            parent.set_child(self)
        self._parent_field = parent_field
        self._children = []
        self._primary_key = None

    #Properties
    @property
    def children(self):
        return self._children

    @property
    def database(self):
        return self._database

    @property
    def fields(self):
        return self._fields

    @property
    def name(self):
        return self._name

    @property
    def parent(self):
        return self._parent

    @property
    def parent_field(self):
        return self._parent_field

    @property
    def table(self):
        return self._table

    @property
    def primary_key(self):
        if self.fields.installed is True:
            if self._primary_key is None:
                self._primary_key = self.database.get_primary_key(self.table)
        else:
            for field in self.fields:
                definition = self.fields[field].definition
                if isinstance(definition, list) and PRIMARY in definition:
                    self._primary_key = field
                    break
        return self._primary_key


    #Methods
    def delete(self, filter):
        self.database.delete(filter=filter, table=self.table)

    def get(self, filter):
        return self.database.select(filter=filter, table=self.table)

    def insert(self, data):
        self.database.insert(data, table=self.table)

    def install(self):
        self.database.create_table(self.table, self.fields, exists=True)
        if self.table not in ("__entities", "__fields"):
            if "__entities" in Entity.persistent[self.database]:
                Entity.persistent[self.database]["__entities"].insert({"name": self.name,
                                                                       "table_name": self.table,
                                                                       "description": self.description,
                                                                       "parent": self.parent and self.parent.table or "",
                                                                       "parent_field": self.parent_field})
            if "__fields" in Entity.persistent[self.database]:
                for field in self.fields:
                    Entity.persistent[self.database]["__fields"].insert({"name": self.fields[field].name,
                                                                         "definition": self.fields[field].definition.__name__,
                                                                         "description": self.fields[field].description,
                                                                         "table_name": self.table})
        self._installed = True
        self.fields.set_installed()

    def replace(self, filter, data):
        self.database.update(data, filter=filter, table=self.table)

    def set_child(self, entity):
        assert isinstance(entity, Entity)
        if entity not in self.children:
            self._children.append(entity)

    def set_database(self, database):
        assert isinstance(database, DBInterface)
        self._database = database
