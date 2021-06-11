# Entities files

from databases.databases import DBInterface
from databases.sqlite import SqliteInterface
from collections import defaultdict
from .fields import Field, Fields
from datetime import datetime, timedelta
from threading import RLock

TIMEOUT = 10
LIFETIME = 600 # Fot future implementation

def set_timeout(timeout):
    TIMEOUT = timeout

class Item(dict):
    persistent = defaultdict(dict)
    def __new__(cls, entity, data={}, loop=None):
        primary_key = entity.primary_key
        assert(primary_key in data)
        if entity in cls.persistent and data[primary_key] in cls.persistent[entity]:
            cls.persistent[entity][data[primary_key]].update_data(data)
            return cls.persistent[entity][data[primary_key]]
        else:
            return super().__new__(cls)

    def __init__(self, entity, data, loop=None):
        super().__init__(data)
        self._primary_key = entity.primary_key
        self._entity = entity
        self._last_event = datetime.now()
        self._last_server_update = datetime.now()
        self._loop = loop
        self._loop_update()
        self._handler = None
        self._server_changed_handlers = defaultdict(list)

    def __del__(self):
        self.close()

    @property
    def entity(self):
        return self._entity

    @property
    def primary_key(self):
        return self._primary_key

    def __setitem__(self, key, value):
        if key in self.entity.fields:
            super().__setitem__(key, value)
            self.entity.replace({self.primary_key: self[self.primary_key]}, {key: value})
            self._last_event = datetime.now()
        else:
            raise Exception("Field not in entity")

    def __getitem__(self, key):
        self._last_event = datetime.now()
        return super().__getitem__(key)

    def _get_from_server(self):
        data = self.entity.get({self.primary_key: self[self.primary_key]})
        if data:
            self.update_data(data[0])
        self._loop_update()

    def _loop_update(self):
        if self._loop:
            self._handler = self._loop.call_soon_threadsafe(lambda: self._loop.call_later(
                                                            TIMEOUT,
                                                            self._get_from_server))

    def changed_handler(self, key):
        return lambda x, key=key: self.__setitem__(key, x)

    def set_handler(self, field, handler):
        self._server_changed_handlers[field].append(handler)

    def remove_handler(self, field, handler):
        if handler in self._server_changed_handlers[field]:
            del(self._server_changed_handlers[field][self._server_changed_handlers[field].index(handler)])

    def close(self):
        if self._handler is not None:
            self._handler.cancel()
        self._loop = None

    def update_data(self, data):
        #TODO: Any verification if needed
        with self.lock:
            self.update(data)
            if self._server_changed_handlers and isinstance(self._server_changed_handlers, dict):
                for key in self.entity.fields:
                    if key in self._server_changed_handlers and key in data and isinstance(self._server_changed_handlers[key], list):
                        for handler in self._server_changed_handlers[key]:
                            if callable(handler) is True:
                                handler(data[key])

class Entity:
    persistent = defaultdict(dict)
    # A dictionary with an entity by database. Why? Suddenly my intuition sais I must do this

    def __new__(cls, database, table, name, fields, description, parent="", parent_field="", loop=None):
        if ":" in table:
            parent, table = table.split(":")[-2:]
            if parent in cls.persistent[database]:
                parent = cls.persistent[database][parent]
                if not "parent_field":
                    parent_field = parent+"_id"
                    if not parent_field in fields:
                        fields[parent_field] = {"definition": parent.primary_key.definition}
            else:
                raise AttributeError("Parent must be previously defined")
        if ":" in name:
            name = name.split(":")[-1]
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

    def __init__(self, database, table, name, fields, description, parent="", parent_field="", loop=None):
        if ":" in table:
            parent, table = table.split(":")[-2:]
            if parent in cls.persistent[database]:
                parent = cls.persistent[database][parent]
                if not "parent_field":
                    parent_field = parent+"_id"
                    if not parent_field in fields:
                        fields[parent_field] = {"definition": parent.primary_key.definition}
            else:
                raise AttributeError("Parent must be previously defined")
        if ":" in name:
            name = name.split(":")[-1]
        assert isinstance(database, DBInterface)
        assert isinstance(fields, dict)
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
        self.lock = RLock()
        self._loop = loop
        self.persistent[database][self.table] = self

    def __getitem__(self, key):
        if isinstance(key, slice):
            if isinstance(key.start, str):
                data = self.get({key.start: key.stop})
            elif isinstance(key.start, int):
                data = self.get({self.primary_key: [(">=", key.start), ("<=", key.stop)]})
            else:
                raise TypeError(f"Only int and string in the first field allowed, {type(key.start)}")
            return [Item(self, item, loop=self._loop) for item in data]
        elif isinstance(key, (int, str)):
            item = self.get({self.primary_key: key})
            if item:
                return [Item(self, item[0], loop=self._loop)]
        else:
            raise TypeError("Only int and slice alloed")

    def __setitem__(self, key, values):
        if isinstance(key, (int, str)):
            item = self.get({self.primary_key: key})
            if item:
                item = Item(self, item[0], loop=self._loop)
                item.update_data(values)
            else:
                values.update({self.primary_key: key})
                self.insert(values)

    def __del__(self):
        self.close()

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
        return self._table.split(":")[-1]

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
    def close(self):
        if self._loop is not None:
            try:
                self._loop.call_soon_threadsafe(self.database.disconnect)
            except RuntimeError:
                pass #It's called always because of __del__
        for item in Item.persistent[self]:
            item.close()

    def delete(self, filter):
        self.database.delete(filter=filter, table=self.table)

    def get(self, filter={}):
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

    def add_field(self, field_dict):
        pass

    def add_fields(self, field_list):
        pass

    def change_field(self, field_id, *, new_field_id=None, new_definition=None, new_description=None):
        if (new_field_id is not None or new_definition is not None) and field_id in self.fields:
            with self.lock:
                new_data = {}
                if new_definition is not None and self.fields[field_id].definition != definition:
                    self.fields[field_id] = new_definition
                    new_data["definition"] = new_definition
                if new_description is not None and self.fields[field_id].description != new_description:
                    self.fields.description = new_description
                    new_data["description"] = new_description
                if new_field_id is not None and new_field_id != field_id:
                    self.fields[field_id].change_name(new_field_id)
                    new_data["name"] = new_field_id
                if new_data and self.installed and "__fields" in Entity.persistent[self.database]:
                    Entity.persistent[self.database]["__fields"].update(new_data,
                                                                        filter={"name": field_id})

    def change_fields(self, fields):
        if isinstance(fields, (list, tuple)):
            for item in fields:
                if isinstance(item, dict):
                    if "new_definition" in item:
                        new_definition = item["new_definition"]
                    if all([key in item for key in ["name", "new_name"]]):
                        new_field_id = item["new_name"]
                    if "new_description" in item:
                        new_description = item["new_description"]
                    self.change_field(item["name"], new_definition=new_definition, new_field_id=new_field_id, new_description=new_description)
