#!/usr/bin/env python

__author__ = "Iván Uría"

"""This module gives an interface "Field" and "Fields" to save memory usage

They are used internally by "Entity".
"""

from collections import defaultdict
from databases import DBInterface
from typing import NoReturn, Union

class Field:
    """Field individual class
    Arguments:
        database: DBInterface object
        table: name of the table
        name: name of the field
        definition: type of the field
        description: description of the field
    Attributes:
        database: DBInterface object
        name: name of the field. Setter defined
        definition: definition of the field
        table: name of the table
    Methods:
        rename: renames the field.
    """
    def __new__(cls, database:DBInterface, table:str, name:str, definition:type, description:str="") -> NoReturn:
        """Defining the new function to search for persistency inside Fields.
        It tries not to duplicate information in memory.
        Arguments:
            database: DBInterface object
            table: name of the table
            name: name of the field
            definition: type of the field
            description: description of the field
        """
        if not table in Fields.persistent[database]:
            Fields(database, table, name)
        if (database in Fields.persistent and
            table in Fields.persistent[database] and
            name in Fields.persistent[database][table]):
            if Fields.persistent[database][table][name].definition == definition:
                return Fields.persistent[database][table][name]
            else:
                return super().__new__(cls)
                #raise Exception ("field {} previously instantiated: {} Vs {}".format(name, definition, Fields.persistent[database][table][name].definition))
        else:
            return super().__new__(cls)

    def __init__(self, database:DBInterface, table:str, name:str, definition:type, description:str="") -> NoReturn:
        """Instantiates Field
        Arguments:
            database: DBInterface object
            table: name of the table
            name: name of the field
            definition: type of the field
            description: description of the field
        """
        self._name = name
        self._database = database
        self._definition = definition
        self.description = description
        self._table = table
        Fields.persistent[database][table][name] = self

    @property
    def database(self) -> DBInterface:
        """Returns DBInterface being used
        """
        return self._database

    @property
    def name(self) -> str:
        """Returns name of the field
        """
        return self._name

    @name.setter
    def name(self, value:str) -> NoReturn:
        """Renames name of the field
        Arguments:
            value: new name for the field
        """
        self.rename(value)

    @property
    def definition(self) -> str:
        """Returns the definition of the field
        """
        return self._definition

    @property
    def table(self) -> str:
        """Returns the name of the table of the field
        """
        return self._table

    def rename(self, new_name:str) -> NoReturn:
        """Renames name of the field
        Arguments:
            value: new name for the field
        """
        if new_name not in Fields.persistent[self.database][self.table]:
            dict.__setitem__(Fields.persistent[self.database][self.table],
                               new_name, self)
            dict.__delitem__(Fields.persistent[self.database][self.table],
                               self.name)
            self.database.alter_table_rename_column(self._name, new_name, table=self.table)
            self._name = new_name

class Fields(dict):
    """dict subclass with a persistent check to save memory.
    It's used internally by Entity.
    Attributes:
        persistent: Class attribute. Dictionary of dictionaries with the form:
            {DBInterface: {table_name: Fields}}
        database: DBInterface to play with the database
        table: name of the table or tree in the database
        fields: a dict of the fields contained. Literally a dict(self)
            {field_name: Field}
        installed: whether or not the database has the required tables.
    Methods:
        All a dict has and...
        set_installed: sets installed to True
    Arguments:
        database: DBInterface to play with
        table: table of the name parenting the Fields
        fields: fields to add. It can be a list of Field:
            [Field, Field, Field...]
            A list of dicts with required data to instantiate a Field:
            [{"name": "field1", "definition": type, "description": "This is optional"}]
            A dict of names of fields with their types:
            {"field1": str, "field2": int, "field3": datetime}
    """
    persistent = defaultdict(dict)
    def __new__(cls, database:DBInterface, table:str, fields:Union[list, tuple, dict]) -> NoReturn:
        """Again __new__ is overriden to check persistency
        Arguments:
            database: DBInterface to play with
            table: table of the name parenting the Fields
            fields: fields to add. It can be a list of Field:
                [Field, Field, Field...]
                A list of dicts with required data to instantiate a Field:
                [{"name": "field1", "definition": type, "description": "This is optional"}]
                A dict of names of fields with their types:
                {"field1": str, "field2": int, "field3": datetime}
        """
        if database in cls.persistent and table in cls.persistent[database]:
            return cls.persistent[database][table]
        else:
            return super().__new__(cls)

    def __init__(self, database:DBInterface, table:str, fields:Union[list, tuple, dict]) -> NoReturn:
        """Initializes new Fields instance
        Arguments:
            database: DBInterface to play with
            table: table of the name parenting the Fields
            fields: fields to add. It can be a list of Field:
                [Field, Field, Field...]
                A list of dicts with required data to instantiate a Field:
                [{"name": "field1", "definition": type, "description": "This is optional"}]
                A dict of names of fields with their types:
                {"field1": str, "field2": int, "field3": datetime}
        """
        super().__init__(self)
        self.persistent[database][table] = self
        self._table = table
        self._database = database
        self._installed = False #To initialize without issues
        if isinstance(fields, dict):
            list(map(lambda x: Field(database, table, x, fields[x]), fields))
        elif (isinstance(fields, (list, tuple)) and
              all([isinstance(item, dict) for item in fields])
              and all([all([key in item for key in ["name", "definition"]]) for item in fields])):
            for item in fields:
                description = "description" in item and item["description"] or ""
                list(map(lambda x: Field(database, table, x["name"], x["definition"], description), fields))

    @property
    def database(self) -> DBInterface:
        """Returns DBInterface instance to play with database
        """
        return self._database

    @property
    def table(self) -> str:
        """Returns the name of the table in the database
        """
        return self._table

    @property
    def fields(self) -> dict:
        """Returns a dict of the fields contained. Literally a dict(self)
            {field_name: Field}
        """
        return dict(self)

    @property
    def installed(self) -> bool:
        """Returns whether or not the database has the required tables
        """
        return self._installed

    #Overrides
    def __setitem__(self, key:str, value:Union[str, dict, type, Field]) -> NoReturn:
        """__setitem__ is overriden to instantiate Field with given data.
        Arguments:
            key: name of the field to set or modify
            value: it can have three kind of values
                str: if the field already exists, it calls rename of the Field with this value
                type: instantiates new Field with given type
                dict: to instantiate Field. Of the kind
                    {"name": "field1", "definition": type, "description": "This is optional"}
                Field: just sets it
        """
        if isinstance(value, Field):
            if self.installed is True:
                if key in self and value.definition != self[key].definition:
                    self.database.alter_table_modify_column(value.name, value.definition, table=self.table)
                elif key not in self:
                    self.database.alter_table_add_column(value.name, value.definition, table=self.table)
            super().__setitem__(key, value)
        else:
            if isinstance(value, type):
                Field(self.database, self.table, key, value)
            elif isinstance(value, dict):
                name = key
                definition = str
                description = ""
                if "name" in value:
                    name = value["name"]
                if "description" in value:
                    description = value["description"]
                if "definition" in value and isinstance(value["definition"], type):
                    definition = value["definition"]
                Field(self.database, self.table, key, name, definition, description=description)
            elif isinstance(value, str) and key in self:
                self[key].rename(value)
            else:
                raise Exception("Operation not permitted")

    def __delitem__(self, key:str) -> NoReturn:
        """Overriden to delete it from database too
        Arguments:
            key: name of the Field
        """
        if key in self and self.installed is True:
            self.database.alter_table_drop_column(key, table=self.table)
        super().__delitem__(key)

    def values(self) -> list:
        """Returns a list of types that can be paired with .keys()
        """
        return [item.definition for item in super().values()]

    ##methods
    def set_installed(self) -> NoReturn:
        """Sets installed to True
        """
        self._installed = True
