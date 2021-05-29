# databases.py

from enum import Enum, auto

class DBEnums(Enum):
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
    def __init__(self, data):
        if isinstance(data, dict):
            data = [data]
        super().__init__(data)

class DBInterface:
    def __init__(self, database="", server="localhost", user="", password="", encription=""):
        """
        Initializes DB Interface
        KWARGS:
            server -> default "localhost"
            database -> db name
            user -> user name to access server
            password -> password to access server
            encription -> if encription is needed
        """
        self._database = database
        self._server = server
        self._user = user
        self._password = password
        self._encription = encription
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
        pass

    def disconnect(self):
        """
        disconnects database
        """
        pass

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
        self.disconnect()

    def _create_sql_query(cls, **kwargs):
        """
        creates sql query with given args
          kwargs may have: "table", "method", "fields", "data"
        """

    def create_table(self, table, fields={}):
        """
        creates table with fields definition
        """

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
