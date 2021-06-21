#!/usr/bin/env python

__author__ = "Iván Uría"

"""This module gives an interface inherited from "DBInterface" to implement SQL
in the project. Intented not to use SQL clauses directly. All data is safe passed.
Example of use:
    db = SQLite(server=MEMORY)
    db.connect()
    data = db.get({"id": 10})
    data.update({"name": "Juan Carlos"}, filter={"id": 10})
    data.insert({"name": "Sofía", "sex": "poor", "age": 70})
    data.delete({"id": 10})
    data.alter_table_rename_column({"name": "alias"})
    ...
"""

import datetime
import re
import sqlite3
import threading
from databases.databases import Data, DBInterface, DBEnums
from collections import defaultdict, OrderedDict

# Constants
MEMORY = ":memory:" # For memory database
RE = re.compile(r"[a-zA-Z0-9 ]+") # Just for search column name and type in schema

def dict_factory(cursor:sqlite3.Cursor, row:list) -> dict:
    """Factory to transform fetching list to dictionary.
    It's used internally by sqlite3.
    Stolen from documentation.
    Arguments:
        cursor: Cursor from sqlite3
        row: List of result from fecthing
    Returns:
        dictionary of the form {"column": value}
    """
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class SqliteInterface(DBInterface):
    """Inherited from DBInterface implements sqlite3 connections with a low use
    of SQL intended.

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
        disconnect: disconnects from database. Must be overriden
        set_database: sets database attribute to indicated argument
        set_table: sets table attribute to indicated table name
        set_filter:  sets filter attribute to indicated filter dictionary
        check_table_exists: check if defined table exists in database
        delete_database: deletes defiend database. Must be overriden
        _create_sql_query: creates an sql query to use by sqlite3.
        create_table: creates indicated table
        drop_table: deletes indicated table.
        select: gets and returns data from table.
        insert: inserts data on table.
        update: updates data from table with indicated filter.
        delete: deletes data from table with indicated filter.
        alter_table_rename_table: renames table or tree.
        alter_table_rename_column: renames column or attribute of a tree.
        alter_table_add_column: adds column to table or attribute to a tree.
        alter_table_modify_column: modifies type of data in a column.
        get_schema: gets data schema.
        get_primary_key: gets the primary key of a table or tree.
    Static Methods:
        _create_filter_query: creates separately a "where" clause. For inner use only.
        _create_fields_pairing: creates separately a pairing key-value clause.
            For inner use only.
        _create_fields_value_for_insert: creates separately a pairing key-value
            clause for insertion. For inner use only.
    """
    def __init__(self, database:str=MEMORY, server:str="localhost", *args) -> NoReturn:
        """Initializes SQLiteInterface
        Arguments:
            server: default "localhost"
            database: db name default :memory:
            user: user name to access server
            password: password to access server
            encryption: if encryption is needed
        """
        super().__init__(database, server, *args)
        self._conn = {}
        self._cursor = {}
        self.connect()

    @property
    def cursor(self) -> sqlite3.Cursor:
        """Returns a cursor for current connection in current thread.
        Threadsafe.
        """
        if not threading.currentThread() in self._cursor:
            self.connect()
        return self._cursor[threading.currentThread()]

    @property
    def conn(self) -> sqlite3.Connection:
        """Returns a connection in current thread.
        Threadsafe.
        """
        if not threading.currentThread() in self._conn:
            self.connect()
        return self._conn[threading.currentThread()]

    # Static Methods
    @classmethod
    def _create_filter_query(cls, filter:dict) -> (str, dict):
        """Creates sql filter query with given filter to use internally by
        _create_sql_query
        Arguments:
            filter: filter to apply, no filter by default
        Returns:
            str with the sql query and dict with safe passing
        """
        assert isinstance(filter, dict)
        if not filter:
            return "", {}
        keys = []
        values = []
        for key in filter.keys():
            if not isinstance(filter[key], (list, tuple)) or len(filter[key]) != 2:
                #filter[key] = ["=", filter[key]]
                keys.append(key)
                values.append(["=", filter[key]])
                continue
            elif isinstance(filter[key], (list, tuple)) and len(filter[key]) == 2 and not isinstance(filter[key][0], (list, tuple)):
                filter[key] = [filter[key]]
            for item in filter[key]:
                if (isinstance(item[0], str) and
                    item[0].upper() in ("=", "!=", "<=", ">=", "<", ">", "LIKE", "IN")):
                    if item[0].upper() == "LIKE":
                        item[0] = " LIKE "
                    if item[0].upper() == "IN":
                        item[0] = " IN "
                    keys.append(key)
                    values.append(item)
                else:
                    raise Exception("Operation not allowed (yet)")
        #keys = list(filter.keys())
        safe = {}
        f = []
        for i, key in enumerate(keys):
            if values[i][0] == " IN " and isinstance(values[i][1], (list, tuple)):
                to_f = []
                for k, v in enumerate(values[i][1]):
                    dakey = ":filter"+key+"value"+str(i)+"in"+str(k)
                    to_f.append(dakey)
                    safe[dakey] = values[i][1][k]
                f.append("("+", ".join(to_f)+")")
            else:
                f.append(":filter"+key+"value"+str(i))
                safe["filter"+key+"value"+str(i)] = values[i][1]
        string = " and ".join([key+values[i][0]+":filter"+key+"value"+str(i) for i, key in enumerate(keys)])
        string = "WHERE {}".format(string)

        return string, safe

    @classmethod
    def _create_fields_pairing(cls, fields:list, data:list, joiner:str=" ") -> (str, dict):
        """Creates sql fields pairing to use internally by _create_sql_query
        Arguments:
            fields: list of fields to pair with data
            data: list of types or data to pair with fields
            joiner: the string between field and data. a space by default ->
                in this case, data must be a list of types
        Returns:
            str with the sql query and dict with safe passing
        """
        assert len(set(fields)) == len(data)
        sql_safe_passing = {}
        pairs = list(zip(fields, data))
        if joiner == " ":
            """
            NULL. The value is a NULL value.
            INTEGER. The value is a signed integer, stored in 1, 2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.
            REAL. The value is a floating point value, stored as an 8-byte IEEE floating point number.
            TEXT. The value is a text string, stored using the database encoding (UTF-8, UTF-16BE or UTF-16LE).
            BLOB. The value is a blob of data, stored exactly as it was input.
            """
            defs = defaultdict(lambda: "TEXT",
                                {DBEnums.PRIMARY: "PRIMARY KEY",
                                str: "TEXT",
                                int: "INTEGER",
                                float: "REAL",
                                None: "NULL",
                                object: "BLOB",
                                datetime.datetime: "timestamp",
                                datetime.date: "date"})
            for index, item in enumerate(pairs):
                if not isinstance(item[1], list) and not isinstance(item[1], tuple):
                    pairs[index] = [item[0], [item[1]]]
                final_item = []
                for definition in pairs[index][1]:
                    final_item.append(defs[definition])
                pairs[index] = [item[0], final_item]
            pairing = ", ".join([joiner.join((item[0], " ".join(item[1]))) for item in pairs])
        else:
            pairing = ", ".join([joiner.join((item[0], ":"+item[0]+"value")) for item in pairs])
            for key, value in pairs:
                sql_safe_passing[key+"value"] = value
        return pairing, sql_safe_passing

    @classmethod
    def _create_fields_value_for_insert(cls, fields:list, values:list) -> (str, dict):
        """Creates sql fields pairing to use internally by _create_sql_query to
        create an insert clause. Separated from _create_fields_value for sanity
        Arguments:
            fields: list of fields to pair with data
            values: list or list of lists of values
        Returns:
            str with the sql query and dict with safe passing
        """
        if isinstance(values, list) and len(values) > 0 and isinstance(values[0], list):
            assert len(fields) == len(values[0])
            safe = []
            for item in values:
                sd = {}
                for index, key in enumerate(fields):
                    sd[key+"value"] = item[index]
                safe.append(sd)
        else:
            assert len(fields) == len(values)
            safe = {}
            for index, key in enumerate(fields):
                safe[key+"value"] = values[index]

        fields_str = ", ".join([key for key in fields])
        values_str = ", ".join([":"+key+"value" for key in fields])
        return fields_str, values_str, safe

    def _create_sql_query(self, *, table:str=None, method:DBEnums=DBEnums.SELECT,
                          fields:[list, tuple]=[], data:[list, tuple]=[],
                          exists:bool=True, filter:dict=None) -> (str, dict):
        """Creates sql query with given kwargs to be used by sqlite3
            You can use self.sql_dict to have a default dictionary for key args.
        Key Arguments:
            table: name of the table. Table set in self.set_table as default
            method: method from DBEnums to use. DBEnums.SELECT as default
            fields: list of fields in order
            data: data to use in the same order than fields if needed
            exists: True by default. Check if table or item exists before executeing clause
            filter: filter to apply, no filter by default.
        Returns:
            str with the sql query and a dictionary with the values to safe passing.
        """
        d = self.sql_dict
        d.update(kwargs)
        kwargs = d
        assert all(["table" in kwargs,
                    "method" in kwargs,
                    "filter" in kwargs,
                    "fields" in kwargs,
                    "data" in kwargs])
        table = kwargs["table"]
        method = kwargs["method"]
        fields = kwargs["fields"]
        if not isinstance(fields, (list, tuple)):
            fields = [fields]
        data = kwargs["data"]
        if not isinstance(data, (list, tuple)):
            data = [data]
        filter = kwargs["filter"]
        if not isinstance(filter, dict):
            filter = {}
        if "exists" in kwargs:
            exists = kwargs["exists"]
        else:
            exists = False
        sql_string = ""
        sql_safe_passing = {}
        template = {DBEnums.SELECT: "SELECT {fields} FROM {table} {where};",
                    DBEnums.INSERT: "INSERT INTO {table} ({fields}) VALUES ({values});",
                    DBEnums.UPDATE: "UPDATE {table} SET {pairing} {where};",
                    DBEnums.DELETE: "DELETE from {table} {where};",
                    DBEnums.CREATE_TABLE: "CREATE TABLE {exists} {table} ({pairing});",
                    DBEnums.CREATE_TABLE_AS_ANOTHER: "CREATE TABLE {exists} {table}",
                    DBEnums.DROP_TABLE: "DROP TABLE IF EXISTS {table}",
                    DBEnums.ALTER_TABLE_ADD_COLUMN: "ALTER TABLE {table} ADD COLUMN {pairing};",
                    DBEnums.ALTER_TABLE_DROP_COLUMN: "ALTER TABLE {table} DROP COLUMN {column};",
                    DBEnums.ALTER_TABLE_RENAME_TABLE: "ALTER TABLE {table} RENAME TO {new_name};",
                    DBEnums.ALTER_TABLE_RENAME_COLUMN: "ALTER TABLE {table} RENAME COLUMN {column} TO {new_name};",
                    DBEnums.GET_SCHEMA: "SELECT * FROM sqlite_master WHERE name = :table;"}

        if method is DBEnums.SELECT:
            if fields and (isinstance(fields, list) or isinstance(fields, tuple)):
                field_str = ", ".join(fields)
            else:
                field_str = "*"
            where_str, sql_safe_passing = self._create_filter_query(filter)
            sql_string = template[method].format(fields=field_str,
                                                where=where_str,
                                                table=table)
        elif method is DBEnums.INSERT:
            fields_str, values_str, sql_safe_passing = self._create_fields_value_for_insert(fields, data)
            sql_string = template[method].format(fields=fields_str,
                                                values=values_str,
                                                table=table)
        elif method is DBEnums.UPDATE:
            pairing, sql_safe_passing = self._create_fields_pairing(fields, data, "=")
            where_str, safe = self._create_filter_query(filter)
            sql_string = template[method].format(pairing=pairing,
                                                 where=where_str,
                                                 table=table)
            sql_safe_passing.update(safe)
        elif method is DBEnums.DELETE:
            where_str, sql_safe_passing = self._create_filter_query(filter)
            sql_string = template[method].format(where=where_str, table=table)
        elif method is DBEnums.CREATE_TABLE:
            pairing, sql_safe_passing = self._create_fields_pairing(fields, data, " ")
            if exists:
                exists_str = "IF NOT EXISTS"
            else:
                exists_str = ""
            sql_string = template[method].format(pairing=pairing,
                                                 exists=exists_str,
                                                 table=table)
        elif method is DBEnums.DROP_TABLE:
            sql_string = template[method].format(table=table)
        elif method is DBEnums.ALTER_TABLE_ADD_COLUMN:
            pairing, sql_safe_passing = self._create_fields_pairing(fields, data, " ")
            sql_string = template[method].format(table=table, pairing=pairing)
        elif method is DBEnums.ALTER_TABLE_DROP_COLUMN:
            sql_string = template[method].format(table=table, column=fields[0])
        elif method is DBEnums.ALTER_TABLE_RENAME_TABLE:
            sql_string = template[method].format(table=table, new_name=data[0])
        elif method is DBEnums.ALTER_TABLE_RENAME_COLUMN:
            sql_string = template[method].format(table=table, column=fields[0], new_name=data[0])
        elif method is DBEnums.GET_SCHEMA:
            sql_string = template[method]
            sql_safe_passing = {"table": table}
        elif method is DBEnums.CREATE_TABLE_AS_ANOTHER:
            if exists:
                exists_str = "IF NOT EXISTS"
            else:
                exists_str = ""
            sql_string = template[method].format(exists=exists_str,
                                                 table=table)
        return sql_string, sql_safe_passing
    # Connection Methods

    def connect(self) -> NoReturn:
        """Connects to database with instantation arguments.
        """
        self._conn[threading.currentThread()] = sqlite3.connect(self._server, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        self._conn[threading.currentThread()].row_factory = dict_factory
        self._cursor[threading.currentThread()] = self._conn[threading.currentThread()].cursor()

    def disconnect(self) -> NoReturn:
        """Disconnects from database.
        """
        self.conn.close()
        del(self._conn[threading.currentThread()])

    # Tables operations
    def create_table(self, table:str, fields:dict={}, data:list=[]], exists:bool=True, database:str=None) -> NoReturn:
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
                 fields also accepts a list with a list of fields, but in this
                 case data argument becomes mandatory
            data: list of types paired with fields. Not necessary if fields is dict
            exists: boolean to add "IF NOT EXISTS" to clause. True by default.
            database: name of database. Database already set by default.
        """
        assert fields # Thay must not be void
        if isinstance (fields, dict):
            data = list(fields.values())
            fields = list(fields.keys())
        if not any([DBEnums.PRIMARY in item for item in data if isinstance(item, (list, tuple))]):
            if not any("id" in item.lower() for item in fields):
                data = [(int, DBEnums.PRIMARY)] + data
                fields = ["id"] + fields
            else:
                index = fields.index("id")
                if isinstance(data, (list, tuple)):
                    data[index] = list(data[index]) + [PRIMARY]
                else:
                    data[index] = (data[index], PRIMARY)
        kwargs = self.sql_dict
        kwargs.update(method=DBEnums.CREATE_TABLE,
                      table=table,
                      fields=fields,
                      data=data,
                      exists=exists)
        sql, safe = self._create_sql_query(**kwargs)
        self.cursor.execute(sql, safe)
        self.conn.commit()

    def drop_table(self, table:str=None, database:str=None) -> NoReturn:
        """Drops selected table
        Arguments:
            table: name of table. Table already set by default.
            database: name of database. Database already set by default.
        Returns:
            table, database
        """
        table, database = super().drop_table(database=database, table=table)
        sql, safe = self._create_sql_query(method=DBEnums.DROP_TABLE,
                                            table=table)
        self.cursor.execute(sql, safe)

    # Executings

    def select(self, filter:dict=None, table:str=None, fields:list=None, database:str=None) -> Data:
        """Selects data in database and table with set_filter
        Arguments:
            filter: filter to use. Filter already set by default
            table: name of table. Table already set by default
            fields: list of fields to get. All fields by default
            database: name of database. Database already set by default
        Returns:
            Data(list of dicts) with results. If only one given a list of len==1
            will be returned
        """
        filter, table, fields, database = super().select(filter, table, fields, database)
        sql, safe = self._create_sql_query(method=DBEnums.SELECT,
                                            table=table,
                                            fields=fields,
                                            filter=filter)
        self.cursor.execute(sql, safe)
        return Data(self.cursor.fetchall())

    def insert(self, data:dict, table:str=None, database:str=None) -> NoReturn:
        """Inserts data in database and table
        Arguments:
            data: dict or list of dicts with the same keys with data to be inserted
            table: name of table. Table already set by default
            database: name of database. Database already set by default
        """
        table, fields, values, database = super().insert(data, database=database, table=table)
        sql, safe = self._create_sql_query(method=DBEnums.INSERT,
                                            table=table,
                                            fields=fields,
                                            data=values)
        if isinstance(safe, dict):
            self.cursor.execute(sql, safe)
        elif isinstance(safe, list):
            self.cursor.executemany(sql, safe)
        self.conn.commit()

    def update(self, data:dict, table:str=None, filter:dict=None, database:str=None) -> NoReturn:
        """Updates data in database and table with given filter
            It prepares from dict in data the lists of values and fields to be
            used in _create_sql_query
        Arguments:
            data: dict with data to be updated
            filter: filter to use. Filter already set by default
            table: name of table. Table already set by default
            database: name of database. Database already set by default
        """
        filter, table, fields, values, database = super().update(data, filter=filter, database=database, table=table)
        sql, safe = self._create_sql_query(method=DBEnums.UPDATE,
                                            table=table,
                                            fields=fields,
                                            data=values,
                                            filter=filter)
        self.cursor.execute(sql, safe)
        self.conn.commit()

    def delete(self, table:str=None, filter:dict=None, database:str=None) -> NoReturn:
        """Removes data in database and table with given filter
        Arguments:
            filter: filter to use. Filter already set by default
            table: name of table. Table already set by default
            database: name of database. Database already set by default
        """
        filter, table, database = super().delete(**kwargs)
        sql, safe = self._create_sql_query(method=DBEnums.DELETE,
                                            table=table,
                                            filter=filter)
        self.cursor.execute(sql, safe)
        self.conn.commit()

    #Table Alterations
    def alter_table_rename_table(self, new_name:str, table:str=None, database:str=None) -> NoReturn:
        """Changes name of table
        Arguments:
            new_name: new name for table
            table: name of table. Table already set by default
            database: name of database. Database already set by default
        """
        table, new_name, database = super().alter_table_rename_table(new_name, table=table)
        sql, safe = self._create_sql_query(method=DBEnums.ALTER_TABLE_RENAME_TABLE,
                                            table=table,
                                            data=[new_name])
        self.cursor.execute(sql, safe)
        self.conn.commit()

    def alter_table_rename_column(self, column:str, new_name:str, table:str=None, database:str=None) -> NoReturn:
        """Changes name of column in table
        Arguments:
            column: real name of column
            new_name: new name for column
            table: name of table. Table already set by default
            database: name of database. Database already set by default
        """
        table, column, new_name, database = super().alter_table_rename_column(column, new_name, table=table)
        sql, safe = self._create_sql_query(method=DBEnums.ALTER_TABLE_RENAME_COLUMN,
                                            table=table,
                                            fields=[column],
                                            data=[new_name])
        self.cursor.execute(sql, safe)
        self.conn.commit()

    def alter_table_add_column(self, column:str, column_type:type, table:str=None, database:str=None) -> NoReturn:
        """Adds new column in table
        Arguments:
            column: name of column
            column_type: python type for this new column
            table: name of table. Table already set by default
            database: name of database. Database already set by default
        """
        table, column, column_type, database = super().alter_table_add_column(column, column_type, table=table)
        sql, safe = self._create_sql_query(method=DBEnums.ALTER_TABLE_ADD_COLUMN,
                                            table=table,
                                            fields=[column],
                                            data=[column_type])
        self.cursor.execute(sql, safe)
        self.conn.commit()

    def alter_table_drop_column(self, column:str, table:str=None, database:str=None) -> NoReturn:
        """Drops columns in table
        Arguments:
            column: name of column
            table: name of table. Table already set by default
            database: name of database. Database already set by default
        """
        #Although defined in sqlite documentation, clause is not recognized. This will be a little tricky
        table, column, database = super().alter_table_drop_column(column, table=table)
        schema = self.get_schema(table=table)
        del(schema[column])
        temp_table = "_temp_"+table
        self.create_table_as_another(temp_table, table=table, fields=list(schema.keys()))
        self.drop_table(table=table)
        self.alter_table_rename_table(table, table=temp_table)


    def alter_table_modify_column(self, column:str, column_type:type, table:str=None, database:str=None) -> NoReturn:
        """Changes data type in specified column
        Arguments:
            column: name of column
            column_type: python type to apply. Data can be lost in this transaction
            table: name of table. Table already set by default
            database: name of database. Database already set by default
        """
        table, column, column_type, database = super().alter_table_modify_column(column, column_type, table=table)
        schema = self.get_schema(table=table)
        schema[column] = column_type
        temp_table = "_temp_"+table
        self.create_table(temp_table, fields=list(schema.keys()), data=list(schema.values()))
        sql = f"INSERT into {temp_table} SELECT * FROM {table}"
        self.conn.execute(sql)
        self.conn.commit()
        self.drop_table(table=table)
        self.alter_table_rename_table(table, table=temp_table)

    #Get SCHEMA
    def get_schema(self, table:str=None, database:str=None) -> NoReturn:
        """Gets Schema for table in database
        Arguments:
            table: name of table. Table already set by default
            database: name of database. Database already set by default
        """
        schema = {"primary key": DBEnums.PRIMARY,
                  "text": str,
                  "integer": int,
                  "real": float,
                  "blob": object,
                  "null": None}
        table, database = super().get_schema(table, database)
        sql, safe = self._create_sql_query(method=DBEnums.GET_SCHEMA,
                                            table=table)
        self.cursor.execute(sql, safe)
        data = RE.findall(self.cursor.fetchone()["sql"])[1:]
        if data[0].strip() == "":
            del(data[0])
        data = OrderedDict([(item[0], " ".join(item[1:])) for item in [string.strip().split(" ") for string in data]])
        returning = OrderedDict()

        for key in data:
            final = []
            prim = []
            data[key] = data[key].lower()
            if "primary key" in data[key]:
                prim.append(DBEnums.PRIMARY)
                data[key] = data[key].replace(" primary key", "").strip()
            items = data[key].split(" ")
            for item in items:
                item = item.lower()
                if item in schema:
                    final.append(schema[item])
            returning[key] = final+prim
        return returning

    def create_table_as_another(self, new_table:str, filter:dict=None, database:str=None, table:str=None, fields:list=None, exists:bool=True) -> NoReturn:
        """Creates new table with indicaed fields and the same content as the
        active table or specified one.
        Arguments:
            new_table: name of the new table
            filter: filter to apply in the copying
            database: name of the database
            table: name of the table to copy
            fields: list of fields to copy
            exists: whether to check if table exists or not. True by default.
        """
        filter, table, fields, database = super().select(filter=filter, database=database, table=table, fields=fields)
        sql, safe = self._create_sql_query(method=DBEnums.SELECT,
                                           table=table,
                                           fields=fields,
                                           filter=filter)
        sql_new, safe_new = self._create_sql_query(method=DBEnums.CREATE_TABLE_AS_ANOTHER,
                                                   table=new_table,
                                                   exists=exists)
        sql = " AS ".join((sql_new, sql))
        self.cursor.execute(sql, safe)
        self.conn.commit()
