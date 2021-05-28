# Entities files

from databases.databases import DBInterface
from databases.sqlite import SqliteInterface
from collections import defaultdict
from .fields import Field, Fields
from datetime import datetime, timedelta

TIMEOUT = 10
LIFETIME = 600

class Item(dict):
    persistent = defaultdict(dict)
    def __new__(cls, entity, data={}):
        primary_key = entity.primary_key
        assert(primary_key in data)
        if entity in cls.persistent and data[primary_key] in cls.persistent[entity]:
            cls.persistent[entity][data[primary_key]].update_data(data)
            return cls.persistent[entity][data[primary_key]]
        else:
            return super().__new__()

    def __init__(self, entity, data, loop=None):
        super.__init__(data)
        self._primary_key = entity.primary_key
        self._entity = Entity
        self._last_event = datetime.now()
        self._last_server_update = datetime.now()
        self._loop = loop
        self._loop_update()
        self._handler = None
        self._server_changed_handlers = None

    @property
    def entity(self):
        return self._entity

    @property
    def primary_key(self):
        return self._primary_key

    def __setitem__(self, key, value):
        if key in self.entity.fields:
            super.__init__(key, value)
            self.entity.replace({self.primary_key: self[self.primary_key]}, {key, value})
            self._last_event = datetime.now()
        else:
            raise Exception("Field not in entity")

    def __getitem__(self, key):
        self._last_event = datetime.now()
        return super().__getitem__(key)

    def __get_from_server(self):
        self.update_data(self.entity.get())

    def __loop_update(self):
        if self._loop:
            self._handler = self._loop.call_soon_threadsafe(lambda: self._loop.call_later(
                                                            TIMEOUT,
                                                            self.__get_from_server))

    def changed_handler(self, key):
        return lambda x, key=key: self.__setitem__(key, x)

    def update_data(self, data):
        #TODO: Any verification if needed
        self.update(data)
        if self._server_changed_handlers is not None and isinstance(self._server_changed_handlers, dict):
            for key in self.entity.fields:
                if key in self.self._server_changed_handlers and key in data:
                    self.self._server_changed_handlers[key](data[key])



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
