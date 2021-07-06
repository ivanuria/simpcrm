#!/usr/bin/env python

__author__ = "Iván Uría"

"""
Classes:
    Item: dict to be given by data
"""
import asyncio
from collections import defaultdict
from datetime import datetime
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
    def __new__(cls, entity:object, data:dict={}, loop:asyncio.BaseEventLoop=None) -> NoReturn:
        """__new__ overriden for checking persistency and saving memory
        Arguments:
            entity: Entity associated
            data: dictionary of data:
                {"field1": "value1", "field2": "value2"...}
            loop: asyncio loop
        """
        primary_key = entity.primary_key
        #assert(primary_key in data) #TODO: Verify if primary_key is inexistent and integer. In this case, go on.
        if entity in cls.persistent and data[primary_key] in cls.persistent[entity]:
            cls.persistent[entity][data[primary_key]].update_data(data)
            return cls.persistent[entity][data[primary_key]]
        else:
            return super().__new__(cls)

    def __init__(self, entity:object, data:dict, loop:asyncio.BaseEventLoop=None) -> NoReturn:
        """Instantiates new Item object. To be used by Entity.
        Don't try to instantiate yourself.
        Arguments:
            entity: Entity associated
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
    def entity(self) -> object:
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
        Arguments:
            data: dictionary of data of kind:
                {"field1": "value1", "field2": "value2"...}
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