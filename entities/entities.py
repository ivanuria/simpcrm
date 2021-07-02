#!/usr/bin/env python

__author__ = "Iván Uría"

"""This module gives an interface to play with entities easily without SQL
Example of new Entity:

    database = SQLite(database="data.db")
    install_persistency()

    loop = asyncio.new_event_loop() # Preparing loop
    t = threading.Thread(target=loop.run_forever) #Preparing a thread for the loop

    customers = Entity(database, "customers", "Customers",
                       {"NID": [str, PRIMARY], "name": str, "birthday": datetime},
                       "Keep your customres satisfied", loop=Loop)
    customers.install()
    customers.insert([{"NID": "12345678H", "name": "Pepi",
                       "birthday": datetime.datetime.fromisoformat('1982-11-04')},
                      {"NID": "11111111K", "name": "Manuel",
                                         "birthday": datetime.datetime.fromisoformat('1969-04-13')}])
    pepi = customers.get({"NID": "12345678H"})[0]
    manuel = customers["11111111K"][0]
    customers.replace({"name": "Pepi"]})
    pepi2 = customers["NID": "11111111K"][0]
    pepi == pepi2 #This is True

Functions:
    set_timeout: sets timeout for the asyncio loop to check if data has changed.
        10 by default.

Classes:
    Item: dict to be given by data
    Entity: An interface to access data in database more friendly
"""

import asyncio
from databases.databases import DBInterface, DBEnums
from databases.sqlite import SqliteInterface
from collections import defaultdict
from .fields import Field, Fields
from datetime import datetime, timedelta
from threading import RLock
from typing import NoReturn, Any, Callable

TIMEOUT = 10
LIFETIME = 600 # Fot future implementation

def set_timeout(timeout:int) -> NoReturn:
    """Sets time to check changes on database
    Arguments:
        timeout: new value. 10 by default
    """
    TIMEOUT = timeout

class Item(dict):
    """dict subclass to able the direct modification of the data.
    Do not instantiate directly, it will be given by Entity class.
    Arguments:
        entity: entity isinstance
        data: dictionary with data
            {"field1": "value1", "field2": "value2"...}
        loop: Asyncio loop to check changes in server periodically
    Attributes:
        persistent: persistency dictionary
            {Entity: {data}}
        entity: the associated entity
        lock: the recursive lock to elude races
        primary_key: the primary key field name
        changed_handler: it returns a lambda to update data in Item
        set_handler: sets a handler to tell the server about changes
        remove_handler: removes a handler that tells the server about changes
        close: closes all connections
    """
    persistent = defaultdict(dict)
    def __new__(cls, entity:Entity, data:dict={}, loop:asyncio.BaseEventLoop=None) -> NoReturn:
        """__new__ overriden for checking persistency and saving memory
        Arguments:
            entity: entity associated
            data: dictionary of data:
                {"field1": "value1", "field2": "value2"...}
            loop: asyncio loop
        """
        primary_key = entity.primary_key
        assert(primary_key in data)
        if entity in cls.persistent and data[primary_key] in cls.persistent[entity]:
            cls.persistent[entity][data[primary_key]].update_data(data)
            return cls.persistent[entity][data[primary_key]]
        else:
            return super().__new__(cls)

    def __init__(self, entity:Entity, data:dict, loop:asyncio.BaseEventLoop=None) -> NoReturn:
        """Instantiates new Item object. To be used by Entity.
        Don't try to instantiate yourself.
        Arguments:
            entity: entity associated
            data: dictionary of data:
                {"field1": "value1", "field2": "value2"...}
            loop: asyncio loop
        """
        super().__init__(data)
        self._primary_key = entity.primary_key
        self._entity = entity
        self._last_event = datetime.now()
        self._last_server_update = datetime.now()
        self._loop = loop
        self._loop_update()
        self._handler = None
        self._server_changed_handlers = defaultdict(list)

    def __del__(self) -> NoReturn:
        """Overriden __del__ for closing connections and clean deletion.
        """
        self.close()

    @property
    def entity(self) -> Entity:
        """Returns assigned entity
        """
        return self._entity

    @property
    def lock(self) -> RLock:
        """Returns the Recursive Lock associated.
        """
        return self.entity.lock

    @property
    def primary_key(self) -> str:
        """Returns the name of the primary key field
        """
        return self._primary_key

    def __setitem__(self, key:str, value:Any) -> NoReturn:
        """Sets item value to key
        Arguments:
            key: key of the field
            value: value to set
        """
        if key in self.entity.fields:
            with self.lock:
                super().__setitem__(key, value)
                self.entity.replace({self.primary_key: self[self.primary_key]}, {key: value})
                self._last_event = datetime.now()
        else:
            raise Exception("Field not in entity")

    def __getitem__(self, key:str) -> Any:
        """Gets the value of the key
        Arguments:
            key: name of the field
        """
        self._last_event = datetime.now()
        return super().__getitem__(key)

    def _get_from_server(self) -> NoReturn:
        """Updates all information from server
        """
        data = self.entity.get({self.primary_key: self[self.primary_key]})
        if data:
            self.update_data(data[0])
        self._loop_update()

    def _loop_update(self) -> NoReturn:
        """Feeds the event lopp with the _get_from_server function
        """
        if self._loop:
            self._handler = self._loop.call_soon_threadsafe(lambda: self._loop.call_later(
                                                            TIMEOUT,
                                                            self._get_from_server))

    def changed_handler(self, key:str) -> NoReturn:
        """Returns the handler to change the data. It can be used by the GUI or
        final API.
        Example:
            item = Entity[1] # This gives the item with ID = 1
            print(item)
            > {"name": "Pepi", "age": 42}
            item.changed_handler("age")(input("Give a new age: "))
            > 33
            print(item)
            > {"name": "Pepi", "age": 33}
        """
        return lambda x, key=key: self.__setitem__(key, x)

    def set_handler(self, field:str, handler:Callable) -> NoReturn:
        """Sets handler to be called in case field changes. It's assigned in a list.
        Attributes:
            field: name of the field
            handler: callable that gets just the new value
        """
        self._server_changed_handlers[field].append(handler)

    def remove_handler(self, field:str, handler:callable) -> NoReturn:
        """Removes handler to be called in case field changes.
        Attributes:
            field: name of the field
            handler: callable to be deleted
        """
        if handler in self._server_changed_handlers[field]:
            del(self._server_changed_handlers[field][self._server_changed_handlers[field].index(handler)])

    def close(self) -> NoReturn:
        """Closes connections with handler and loop. It's called from __del__
        """
        if self._handler is not None:
            self._handler.cancel()
        self._loop = None

    def update_data(self, data:dict) -> NoReturn:
        """Updates all data given in Item
        """
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
            if parent in self.persistent[database]:
                parent = self.persistent[database][parent]
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
                return Item(self, item[0], loop=self._loop)
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
        data = self.database.select(filter=filter, table=self.table)
        return [Item(self, item, loop=self._loop) for item in data]

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
                    if not isinstance(self.fields[field].definition, (list, tuple)):
                        definition = self.fields[field].definition.__name__
                    else:
                        primary = []
                        if DBEnums.PRIMARY in self.fields[field].definition:
                            del(self.fields[field].definition[self.fields[field].definition.index(DBEnums.PRIMARY)])
                            primary = ["DBEnums.PRIMARY"]
                        definition = ",".join([i.__name__ for i in self.fields[field].definition]+primary)
                    Entity.persistent[self.database]["__fields"].insert({"name": self.fields[field].name,
                                                                         "definition": definition,
                                                                         "description": self.fields[field].description,
                                                                         "table_name": self.table})
        self._installed = True
        self.fields.set_installed()

    def uninstall(self):
        if self.table not in ("__entities", "__fields"):
            if "__fields" in Entity.persistent[self.database]:
                Entity.persistent[self.database]["__fields"].delete({"table_name": self.table})
            if "__entities" in Entity.persistent[self.database]:
                Entity.persistent[self.database]["__entities"].delete({"table_name": self.table})
            if self.table in Entity.persistent[self.database]:
                self.database.drop_table(self.table)
            del(self)

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
