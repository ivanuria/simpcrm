# Entities files

from databases.databases import DBInterface
from databases.sqlite import SqliteInterface
from collections import defaultdict
from .fields import Field, Fields


class Entity:
    persistent = defaultdict(dict)
    # A dictionary with an entity by database. Why? Suddenly my intuition sais I must do this

    def __new__(cls, database, table, name, fields, description, parent=None, parent_field="", installed=False):
        if table in cls.persistent[database]:
            self = cls.persistent[database][table]
            if any(name!=self.name,
                   fields!=self.fields_orig,
                   parent!=self.parent,
                   parent_field!=self.parent_field):
                raise Exception("Entity already defined")
            else:
                return self
        else:
            return super().__new__(cls)

    def __init__(self, database, table, name, fields, description, parent=None, parent_field="", installed=False):
        assert isinstance(database, DBInterface)
        assert table not in self.persistent[database]
        self.persistent[database][table] = self
        self._name = name
        self._table = table
        self.description = description
        self._fields_orig = fields
        self._fields = Fields(database, table, fields, installed=installed)
        self._database = database
        self._parent = parent
        if isinstance(parent, Entity):
            parent.set_child(self)
        self._parent_field = parent_field
        self._children = []

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

    #Methods
    def delete(self, filter):
        self.database.delete(filter=filter, table=table)

    def get(self, filter):
        return self.database.select(filter=filter, table=self.table)

    def insert(self, data):
        self.database.insert(data, table=self.table)

    def install(self):
        self.database.create_table(self.table, self.fields, exists=True)
        if self.table not in ("__entities", "__fields"):
            if "__entities" in Entity.persistent:
                Entity.persistent["__entities"].insert({"name": self.name,
                                                     "table": self.table,
                                                     "description": self.description,
                                                     "parent": self.parent.table,
                                                     "parent_field": self.parent_field})
            if "__fields" in Entity.persistent:
                for field in self.fields:
                    Entity.persistent["__fields"].insert({"name": field.name,
                                                       "definition": str(field.definition),
                                                       "description": field.description,
                                                       "table": self.table})

    def replace(self, filter, data):
        self.database.update(self, data, filter=filter, table=table)

    def set_child(self, entity):
        assert isinstance(entity, Entity)
        if entity not in self.children:
            self._children.append(entity)

    def set_database(self, database):
        assert isinstance(database, DBInterface)
        self._database = database
