#!/usr/bin/env python

__author__ = "Iván Uría"

"""
databases.py gives an interface "DBInterface" to inherit from to develop any kind
of database interface
"""

from enum import Enum, auto

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
    """
    def __init__(self, database="", server="localhost", user="", password="", encryption=""):
        """
        Initializes DB Interface
        KWARGS:
            server -> default "localhost"
            database -> db name
            user -> user name to access server
            password -> password to access server
            encryption -> if encription is needed
        """
        self._database = database
        self._server = server
        self._user = user
        self._password = password
        self._encryption = encription
        self._table = ""
        self._filter = {}

    @property
    def database(self):
        """
        returns database name
        """
        return self._database

    @property
    def server(self):
        """
        returns server name
        """
        return self._server

    @property
    def table(self):
        """
        returns active table name
        """
        return self._table

    @property
    def filter(self):
        """
        returns active filter
        """
        return self._filter

    @property
    def sql_dict(self):
        return {"table": self.table,
                "filter": self.filter,
                "method": DBEnums.SELECT,
                "fields": [],
                "data": [],
                "exists":True
                }

    def connect(self):
        """
        connects to database
        """
        raise NotImplementedError

    def disconnect(self):
        """
        disconnects database
        """
        raise NotImplementedError

    def set_database(self, database):
        """
        sets database name
        """
        self._database = database

    def set_table(self, table):
        """
        sets table name
        """
        self._table = table

    def set_filter(self, filter):
        """
        sets filter
        """
        self._filter = filter

    def check_table_exists(self, table, database=None):
        """
        checkes if table exists
        """
        pass

    def delete_database(self, database=None):
        """
        disconnects and deletes selectted database
        """
        raise NotImplementedError

    def _create_sql_query(cls, **kwargs):
        """
        creates sql query with given args
          kwargs may have: "table", "method", "fields", "data"
        """
        raise NotImplementedError

    def create_table(self, table, fields={}):
        """
        creates table with fields definition
        """
        raise NotImplementedError

    def drop_table(self, database=None, table=None):
        """
        drops selected table
        """
        if database is None:
            database = self.database
        if table is None:
            table = self.table
        return database, table

    def select(self, filter=None, database=None, table=None, fields=None, **kwargs):
        """
        selects data in database and table with set_filter
        """
        if filter is None:
            filter = self.filter
        if database is None:
            database = self.database
        if table is None:
            table = self.table
        if fields is None:
            fields = []
        return filter, database, table, fields

    def insert(self, data, database=None, table=None):
        """
        inserts data in database and table
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
        return database, table, fields, values

    def update(self, data, filter=None, database=None, table=None):
        """
        inserts data in database and table with set_filter
        """
        assert isinstance(data, dict)
        if filter is None:
            filter = self.filter
        if database is None:
            database = self.database
        if table is None:
            table = self.table
        fields, values = list(data.keys()), list(data.values())
        return filter, database, table, fields, values

    def delete(self, filter=None, database=None, table=None):
        """
        removes data in database and table with set_filter
        """
        if filter is None:
            filter = self.filter
        if database is None:
            database = self.database
        if table is None:
            table = self.table
        return filter, database, table

    #Table Alterations
    def alter_table_rename_table(self, new_name, table=None):
        """
        Changes name of table
        """
        if table is None:
            table = self.table
        self.set_table(new_name)
        return table, new_name

    def alter_table_rename_column(self, column, new_name, table=None):
        """
        Changes name of column is specified table table
        """
        if table is None:
            table = self.table
        return table, column, new_name

    def alter_table_add_column(self, column, column_type, table=None):
        """
        Adds new column in specified table table
        """
        if table is None:
            table = self.table
        return table, column, column_type

    def alter_table_drop_column(self, column, table=None):
        """
        Adds new column in specified table table
        """
        if table is None:
            table = self.table
        return table, column

    def alter_table_modify_column(self, column, column_type, table=None):
        """
        Changes data type for column in specified table table
        """
        if table is None:
            table = self.table
        return table, column, column_type

    #Get SCHEMA
    def get_schema(self, table=None):
        if table is None:
            table = self.table
        return table

    def get_primary_key(self, table=None):
        if table is None:
            table = self.table
        schema = self.get_schema(table=table)
        for item in schema:
            if isinstance(schema[item], list) and DBEnums.PRIMARY in schema[item]:
                return item
