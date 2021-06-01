# sqlite.py file inheriting from databases.py

import sqlite3
from databases.databases import Data, DBInterface, DBEnums
from collections import defaultdict, OrderedDict
import re
import threading
import datetime

MEMORY = ":memory:"
RE = re.compile(r"[a-zA-Z0-9 ]+")

CREATE_TABLE_AS_ANOTHER = "CREATE TABLE AS ANOTHER"

def dict_factory(cursor, row): # Stolen from documentation
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class SqliteInterface(DBInterface):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._conn = {}
        self._cursor = {}
        self.connect()

    @property
    def cursor(self):
        if not threading.currentThread() in self._cursor:
            self.connect()
        return self._cursor[threading.currentThread()]

    @property
    def conn(self):
        if not threading.currentThread() in self._conn:
            self.connect()
        return self._conn[threading.currentThread()]

    # Static Methods

    @classmethod
    def _create_filter_query(cls, filter):
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
                    item[0].upper() in ("=", "!=", "<=", ">=", "<", ">", "LIKE")):
                    if item[0].upper() == "LIKE":
                        item[0] = " LIKE "
                    keys.append(key)
                    values.append(item)
                else:
                    raise Exception("Operation not allowed (yet)")
        #keys = list(filter.keys())
        string = " and ".join([key+values[i][0]+":filter"+key+"value"+str(i) for i, key in enumerate(keys)])
        string = "WHERE {}".format(string)
        safe = {}
        for i,key in enumerate(keys):
            #safe["filter"+key+"value"+str(i)] = filter[key][1]
            safe["filter"+key+"value"+str(i)] = values[i][1]
        return string, safe

    @classmethod
    def _create_fields_pairing(cls, fields, data, joiner=" "):
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
                                datetime.date: "date",
                                datetime.timedelta: "timestamp"})
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
    def _create_fields_value_for_insert(cls, fields, values):
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

    def _create_sql_query(self, **kwargs):
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

    def connect(self):
        self._conn[threading.currentThread()] = sqlite3.connect(self._server, , detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        self._conn[threading.currentThread()].row_factory = dict_factory
        self._cursor[threading.currentThread()] = self._conn[threading.currentThread()].cursor()

    def disconnect(self):
        self.conn.close()
        del(self._conn[threading.currentThread()])

    # Tables operations
    def create_table(self, table, fields={}, data={}, exists=True):
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

    def drop_table(self, table, database=None):
        """
        drops selected table
        """
        database, table = super().drop_table(database=database, table=table)
        sql, safe = self._create_sql_query(method=DBEnums.DROP_TABLE,
                                            table=table)
        self.cursor.execute(sql, safe)
        return Data(self.cursor.fetchall())

    # Executings

    def select(self, **kwargs):
        filter, database, table, fields = super().select(**kwargs)
        sql, safe = self._create_sql_query(method=DBEnums.SELECT,
                                            table=table,
                                            fields=fields,
                                            filter=filter)
        self.cursor.execute(sql, safe)
        return Data(self.cursor.fetchall())

    def insert(self, data, database=None, table=None):
        database, table, fields, values = super().insert(data, database=database, table=table)
        sql, safe = self._create_sql_query(method=DBEnums.INSERT,
                                            table=table,
                                            fields=fields,
                                            data=values)
        if isinstance(safe, dict):
            self.cursor.execute(sql, safe)
        elif isinstance(safe, list):
            self.cursor.executemany(sql, safe)
        self.conn.commit()

    def update(self, data, filter=None, database=None, table=None):
        filter, database, table, fields, values = super().update(data, filter=filter, database=database, table=table)
        sql, safe = self._create_sql_query(method=DBEnums.UPDATE,
                                            table=table,
                                            fields=fields,
                                            data=values,
                                            filter=filter)
        self.cursor.execute(sql, safe)
        self.conn.commit()

    def delete(self, **kwargs):
        filter, database, table = super().delete(**kwargs)
        sql, safe = self._create_sql_query(method=DBEnums.DELETE,
                                            table=table,
                                            filter=filter)
        self.cursor.execute(sql, safe)
        self.conn.commit()

    #Table Alterations
    def alter_table_rename_table(self, new_name, table=None):
        """
        Changes name of table
        """
        table, new_name = super().alter_table_rename_table(new_name, table=table)
        sql, safe = self._create_sql_query(method=DBEnums.ALTER_TABLE_RENAME_TABLE,
                                            table=table,
                                            data=[new_name])
        self.cursor.execute(sql, safe)
        self.conn.commit()

    def alter_table_rename_column(self, column, new_name, table=None):
        """
        Changes name of column is specified table table
        """
        table, column, new_name = super().alter_table_rename_column(column, new_name, table=table)
        sql, safe = self._create_sql_query(method=DBEnums.ALTER_TABLE_RENAME_COLUMN,
                                            table=table,
                                            fields=[column],
                                            data=[new_name])
        self.cursor.execute(sql, safe)
        self.conn.commit()

    def alter_table_add_column(self, column, column_type, table=None):
        """
        Adds new column in specified table table
        """
        table, column, column_type = super().alter_table_add_column(column, column_type, table=table)
        sql, safe = self._create_sql_query(method=DBEnums.ALTER_TABLE_ADD_COLUMN,
                                            table=table,
                                            fields=[column],
                                            data=[column_type])
        self.cursor.execute(sql, safe)
        self.conn.commit()

    def alter_table_drop_column(self, column, table=None):
        """
        Adds new column in specified table table
        """
        #Although defined in sqlite documentation, clause is not recognized. This will be a little tricky
        table, column = super().alter_table_drop_column(column, table=table)
        schema = self.get_schema(table=table)
        del(schema[column])
        temp_table = "_temp_"+table
        self.create_table_as_another(temp_table, table=table, fields=list(schema.keys()))
        self.drop_table(table=table)
        self.alter_table_rename_table(table, table=temp_table)


    def alter_table_modify_column(self, column, column_type, table=None):
        """
        Changes data type for column in specified table table
        """
        table, column, column_type = super().alter_table_modify_column(column, column_type, table=table)
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
    def get_schema(self, table=None):
        schema = {"primary key": DBEnums.PRIMARY,
                  "text": str,
                  "integer": int,
                  "real": float,
                  "blob": object,
                  "null": None}
        table = super().get_schema(table=table)
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

    def create_table_as_another(self, new_table, filter=None, database=None, table=None, fields=None, exists=True):
        filter, database, table, fields = super().select(filter=filter, database=database, table=table, fields=fields)
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
