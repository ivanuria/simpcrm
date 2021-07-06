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
    Entity: An interface to access data in database more friendly
"""

from databases.databases import DBInterface, DBEnums
from collections import defaultdict
from .fields import Fields
from .items import Item, set_timeout, TIMEOUT
from threading import RLock
from typing import NoReturn, Any, Callable


class Entity:
    """Entity represents a table or tree of data. It gives an interface to play
    with the data.
    Use it directly only to create new Entities. For existing entities in database
    use get_entity or get_entities instead.
    Arguments:
        database: DBInterface to play with
        table: name of the table
        name: name of the entity
        fields: fields to use use. See Fields doc.
        description: desciption of the entity
        parent: name of the parent table of this entity
        parent_field: name of the field representing the parent
        loop: event loop to check changes
    Atributes:
        children: list of entities depending on this entity
        database: DBInterface associated
        fields: Fields object of associated fields
        name: name of the entity
        parent: name of the parent
        parent_field: name of the field associated to parent (must be PRIMARY)
        table: name of the table
        primary_key: name of the field wich is primary key
    Methods:
        close: closes connections. Called from __del__
        delete: deletes data from database
        get: returns a list of Item
        insert: insert data in database
        install: installs in database
        uninstall: removes table from database and self from memory
        replace: changes data from database
        set_child: appends a child to children
        set_database: sets new database
        add_field: adds a new field and changes database if needed
        change_field: changes a field configuration
        change_fields: changes fields configurations
    Example:
        sqlite = databases.SQlite("data.db")
        customers = Entity(sqlite, "customers", "Customers", {"name": str, "age": int},
                           "My customers satisfied")
        customers.install()
        customers.insert([{"name": "Pepi", "age": 32}, {"name": "George", "age":22}])
        pepi = customers[1]
        george = customers[2]
        customers.update({"name": "George"}, {"age": 24})
    """
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
        return self._children.copy()

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
                if isinstance(definition, list) and DBEnums.PRIMARY in definition:
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
                if new_definition is not None and self.fields[field_id].definition != new_definition:
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
