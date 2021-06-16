#!/usr/bin/env python

__author__ = "Iván Uría"

"""This module gives an interface "DBInterface" to inherit from to develop any kind
of database interface.

Example:
    class MySQLInterface(DBInterface):
        def __init__(self, database="", server="localhost", user="", password="", encryption=""):
            super().__init__(database, server, user, password, enryption)
            #Other important stuff to your implementation
        def connect(self):
            #Do stuff to connect to database
        def disconnect(self):
            #Do stuff to disconnect
        ...
"""

from enum import Enum, auto
from typing import NoReturn

class DBEnums(Enum):
    """Enumerator of Constants used by database objects.

    Attributes:
        SELECT: Indicator for select clauses in sql or any database retreving data
        INSERT: Indicator for insert clauses in sql or any database insertion data
        UPDATE: Indicator for update clauses in sql or any database replacing data
        DELETE: Indicator for delete clauses in sql or any database deleting data
        CREATE_TABLE: Indicator for table creation in sql or any database tree creation
        CREATE_TABLE_AS_ANOTHER: Indicator for table creation and copying data from another one
        DROP_TABLE: Indicator for table deletion in sql or any database tree deletion
        ALTER_TABLE_ADD_COLUMN: Indicator for adding a column in sql
        ALTER_TABLE_DROP_COLUMN: Indicator for deleting a column in sql
        ALTER_TABLE_RENAME_TABLE: Indicator for renaming a table in sql or any tree renaming
        ALTER_TABLE_RENAME_COLUMN: Indicator for renaming a column in sql
        ALTER_TABLE_MODIFY_COLUMN: Indicator for changing type of a column in sql
        GET_SCHEMA: Indicator for getting the schema in sql
        PRIMARY: Indicator for adding a column in sql
    """
    SELECT = auto()
    INSERT = auto()
    UPDATE = auto()
    DELETE = auto()
    CREATE_TABLE = auto()
    DROP_TABLE = auto()
    ALTER_TABLE_ADD_COLUMN = auto()
    ALTER_TABLE_DROP_COLUMN = auto()
    ALTER_TABLE_RENAME_TABLE = auto()
    ALTER_TABLE_RENAME_COLUMN = auto()
    ALTER_TABLE_MODIFY_COLUMN = auto()
    GET_SCHEMA = auto()
    PRIMARY = auto()
    CREATE_TABLE_AS_ANOTHER = auto()

class Data(list): #not checked datatypes
    """Custom list inherited class to check if it has been checked or not
    """
    def __init__(self, data):
        if isinstance(data, dict):
            data = [data]
        super().__init__(data)

class DBInterface:
    """Semi-abstract class to derive implementations to access any kind of databases

    Attributes:
        database: name of the database
        server: server path
        table: active table or tree
        filter: active filter
        sql_dict: default dictionary to pass to use in inner methods with relevant
            information. Defaults are:
                {"table": self.table,
                 "filter": self.filter,
                 "method": DBEnums.SELECT,
                 "fields": [],
                 "data": [],
                 "exists":True
                 }
    Arguments:
        database: (str) name of the database
        server: (str) server path
        user: (str)  user name if required
        password: (str) password if required
        encryption: (str) if exncryption is required -> implementation not defined yet
    Methods:
        connect: connects to database with instance defined attributes.
            Must be overriden
        disconnect: disconnects from database. Must be overriden
        set_database: sets database attribute to indicated argument
        set_table: sets table attribute to indicated table name
        set_filter:  sets filter attribute to indicated filter dictionary
        check_table_exists: check if defined table exists in database
        delete_database: deletes defiend database. Must be overriden
        _create_sql_query: creates an sql query if needed. Must be overriden
        create_table: creates indicated table
        drop_table: deletes indicated table. Must be called from super() on overriding
        select: gets and returns data from table. Must be called from super() on overriding
        insert: inserts data on table. Must be called from super() on overriding
        update: updates data from table with indicated filtera.
            Must be called from super() on overriding
        delete: deletes data from table with indicated filtera.
            Must be called from super() on overriding
        alter_table_rename_table: renames table or tree
            Must be called from super() on overriding
        alter_table_rename_column: renames column or attribute of a tree
            Must be called from super() on overriding
        alter_table_add_column: adds column to table or attribute to a tree
            Must be called from super() on overriding
        alter_table_modify_column: modifies type of data in a column
            Must be called from super() on overriding
        get_schema: gets data schema
            Must be called from super() on overriding
        get_primary_key: gets the primary key of a table or tree
            Must be called from super() on overriding
    """
    def __init__(self, database:str="", server:str="localhost", user:str="", password:str="", encryption:str="") -> NoReturn:
        """Initializes DB Interface
        Arguments:
            server: default "localhost"
            database: db name
            user: user name to access server
            password: password to access server
            encryption: if encryption is needed
        """
        self._database = database
        self._server = server
        self._user = user
        self._password = password
        self._encryption = encryption
        self._table = ""
        self._filter = {}

    @property
    def database(self) -> str:
        """Returns database name
        """
        return self._database

    @property
    def server(self) -> str:
        """Returns server name
        """
        return self._server

    @property
    def table(self) -> str:
        """Returns active table name
        """
        return self._table

    @property
    def filter(self) -> dict:
        """Returns active filter
        """
        return self._filter

    @property
    def sql_dict(self) -> dict:
        """Returns a dcitionary with specified items that will be needed in implementations
        For sanity sake
        """
        return {"table": self.table,
                "filter": self.filter,
                "method": DBEnums.SELECT,
                "fields": [],
                "data": [],
                "exists":True
                }

    def connect(self) -> NoReturn:
        """Connects to database.
        To be implemented in child class.
        """
        raise NotImplementedError

    def disconnect(self) -> NoReturn:
        """Disconnects database.
        To be implemented in child class.
        """
        raise NotImplementedError

    def set_database(self, database:str) -> NoReturn:
        """Sets database name.
        Arguments:
            database: name of the database or file
        """
        self._database = database

    def set_table(self, table:str) -> NoReturn:
        """Sets table name
        Arguments:
            table: name of the table or the tree
        """
        self._table = table

    def set_filter(self, filter:dict) -> NoReturn:
        """Sets filter as specified
        Arguments:
            filter: Dcitionary with the filter. It will follow this form:
                {"FIELD NAME": ["OPERATION TYPE", "VALUE"],
                 "FIELD NAME": "VALUE"}
                FIELD NAME: the name of the field.
                OPERATION TYPE: "=" by default if only given the VALUE.
                    It has to accept all of this ("=", "!=", "<=", ">=", "<", ">", "LIKE", "IN")
                VALUE: The value to check
            All items in the same dictionary will be trated with the boolean operation of "AND".
        """
        self._filter = filter

    def check_table_exists(self, table:str, database:str=None) -> bool:
        """Checks if table exists.
        To be implemented in child class.
        Arguments:
            table: Name of the table to check
            database: Name of the database to check. If None, the default database
                set with `set_database` will be set.
        Returns:
            True: Table exists
            False: Table doesn't exists
        """
        raise NotImplementedError

    def delete_database(self, database:str=None) -> NoReturn:
        """Disconnects and deletes selectted database
        To be implemented in child class.
        Arguments:
            database: Name of the database to check. If None, the default database
                set with `set_database` will be set.
        """
        raise NotImplementedError

    def _create_sql_query(self, *, table:str=None, method:DBEnums=DBEnums.SELECT,
                          fields:[list, tuple]=[], data:[list, tuple]=[],
                          exists:bool=True, filter:dict=None) -> str:
        """Creates sql query with given args
            You can use self.sql_dict to have a default dictionary
            To be implemented in child class.
        Arguments:
            table: name of the table. Table set in self.set_table as default
            method: method from DBEnums to use. DBEnums.SELECT as default
            fields: list of fields in order
            data: data to use in the same order than fields if needed
            exists: True by default. Check if table or item exists before executeing clause
            filter: filter to apply, no filter by default so you can espicify it in implementation
        Returns:
            str with the sql query.
        """
        raise NotImplementedError

    def create_table(self, table:str, fields:dict={}, database:str=None) -> NoReturn:
        """Creates table with fields definition
            To be implemented in child class.
        Arguments:
            table: name of the table
            fields: dict with the name of the filed and the python type associated
                This type must be converted to database specification in SQL clause
                Example:
                {"id": [int, DBEnums.PRIMARY],
                 "foo": str,
                 "bar": datatime.datetime}
            database: name of database. Database already set by default.
        """
        raise NotImplementedError

    def drop_table(self, table=None, database=None) -> tuple:
        """Drops selected table
            To be overriden in child class, to use defaults given by this class use:
                table, database = super().drop_table(table, database)
        Arguments:
            table: name of table. Table already set by default.
            database: name of database. Database already set by default.
        Returns:
            table, database
        """
        if database is None:
            database = self.database
        if table is None:
            table = self.table
        return table, database

    def select(self, filter:dict=None, table:str=None, fields:list=None, database:str=None) -> tuple:
        """Selects data in database and table with set_filter
            To be overriden in child class, to use defaults given by this class use:
                filter, table, fields, database = super().select(filter, table, fields, database)
        Arguments:
            filter: filter to use. Filter already set by default
            table: name of table. Table already set by default
            fields: list of fields to get. All fields by default
            database: name of database. Database already set by default
        Returns:
            filter, table, fields, database
        """
        if filter is None:
            filter = self.filter
        if database is None:
            database = self.database
        if table is None:
            table = self.table
        if fields is None:
            fields = []
        return filter, table, fields, database

    def insert(self, data:[dict, list, tuple], table:str=None, database:str=None) -> tuple:
        """Inserts data in database and table
            To be overriden in child class, to use defaults given by this class use:
                table, fields, values, database = super().insert(data, table, database)
            It prepares from dict in dada the lists of values and fields to be
            used in _create_sql_query
        Arguments:
            data: dict or list of dicts with the same keys with data to be inserted
            table: name of table. Table already set by default
            database: name of database. Database already set by default
        Returns:
            filter, table, fields, database
        """
        if database is None:
            database = self.database
        if table is None:
            table = self.table
        if isinstance(data, dict):
            fields, values = list(data.keys()), list(data.values())
        elif isinstance(data, (list, tuple)):
            fields = list(data[0].keys())
            values = [list(item.values()) for item in data]
        return table, fields, values, database

    def update(self, data:dict, filter:dict=None, table:str=None, database:str=None) -> tuple:
        """Updates data in database and table with given filter
            To be overriden in child class, to use defaults given by this class use:
                filter, table, fields, values, database = super().update(data, filter, table, database)
            It prepares from dict in dada the lists of values and fields to be
            used in _create_sql_query
        Arguments:
            data: dict with data to be updated
            filter: filter to use. Filter already set by default
            table: name of table. Table already set by default
            database: name of database. Database already set by default
        Returns:
            filter, table, fields, values, database
        """
        assert isinstance(data, dict)
        if filter is None:
            filter = self.filter
        if database is None:
            database = self.database
        if table is None:
            table = self.table
        fields, values = list(data.keys()), list(data.values())
        return filter, table, fields, values, database

    def delete(self, filter:dict=None, table:str=None, database:str=None) -> tuple:
        """Removes data in database and table with given filter
            To be overriden in child class, to use defaults given by this class use:
                filter, table, database = super().delete(filter, table, database)
        Arguments:
            filter: filter to use. Filter already set by default
            table: name of table. Table already set by default
            database: name of database. Database already set by default
        Returns:
            filter, table, database
        """
        if filter is None:
            filter = self.filter
        if database is None:
            database = self.database
        if table is None:
            table = self.table
        return filter, table, database

    #Table Alterations
    def alter_table_rename_table(self, new_name:str, table:str=None, database:str=None) -> tuple:
        """Changes name of table
            To be overriden in child class, to use defaults given by this class use:
                table, new_name, database = super().alter_table_rename_table(table, new_name, database)
        Arguments:
            new_name: new name for table
            table: name of table. Table already set by default
            database: name of database. Database already set by default
        Returns:
            table, new_name, database
        """
        if table is None:
            table = self.table
        if database is None:
            database = self.database
        self.set_table(new_name)
        return table, new_name, database

    def alter_table_rename_column(self, column:str, new_name:str, table:str=None, database:str=None) -> tuple:
        """Changes name of column in table
            To be overriden in child class, to use defaults given by this class use:
                table, column, new_name, database = super().alter_table_rename_column(table, column, new_name, database)
        Arguments:
            column: real name of column
            new_name: new name for column
            table: name of table. Table already set by default
            database: name of database. Database already set by default
        Returns:
            table, column, new_name, database
        """
        if table is None:
            table = self.table
        if database is None:
            database = self.database
        return table, column, new_name, database

    def alter_table_add_column(self, column:str, column_type:type, table:str=None, database:str=None) -> tuple:
        """Adds new column in table
            To be overriden in child class, to use defaults given by this class use:
                table, column, column_type, database = super().alter_table_add_column(table, column, column_type, database)
        Arguments:
            column: name of column
            column_type: python type for this new column
            table: name of table. Table already set by default
            database: name of database. Database already set by default
        Returns:
            table, column, column_type, database
        """
        if table is None:
            table = self.table
        if database is None:
            database = self.database
        return table, column, column_type, database

    def alter_table_drop_column(self, column:str, table:str=None, database:str=None) -> tuple:
        """Drops columns in table
            To be overriden in child class, to use defaults given by this class use:
                table, column, database = super().alter_table_drop_column(table, column, database)
        Arguments:
            column: name of column
            table: name of table. Table already set by default
            database: name of database. Database already set by default
        Returns:
            table, column, column_type, database
        """
        if table is None:
            table = self.table
        if database is None:
            database = self.database
        return table, column, database

    def alter_table_modify_column(self, column:str, column_type:type, table:str=None, database:str=None) -> tuple:
        """Changes data type in specified column
            To be overriden in child class, to use defaults given by this class use:
                table, column, database = super().alter_table_drop_column(table, column, database)
        Arguments:
            column: name of column
            column_type: python type to apply. Data can be lost in this transaction
            table: name of table. Table already set by default
            database: name of database. Database already set by default
        Returns:
            table, column, column_type, database
        """
        if table is None:
            table = self.table
        if database is None:
            database = self.database
        return table, column, column_type, database

    #Get SCHEMA
    def get_schema(self, table:str=None, database:str=None) -> tuple:
        """Gets Schema for table in database
            To be overriden in child class, to use defaults given by this class use:
                table, database = super().get_schema(table, database)
        """
        if table is None:
            table = self.table
        if database is None:
            database = self.database
        return table, database

    def get_primary_key(self, table=None):
        if table is None:
            table = self.table
        schema = self.get_schema(table=table)
        for item in schema:
            if isinstance(schema[item], list) and DBEnums.PRIMARY in schema[item]:
                return item
