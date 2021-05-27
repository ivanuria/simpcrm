# sqlite.py file inheriting from databases.py

import sqlite3
from databases.databases import Data, DBInterface
from databases.databases import SELECT, INSERT, UPDATE, DELETE, CREATE_TABLE, DROP_TABLE
from databases.databases import PRIMARY
from collections import defaultdict

MEMORY = ":memory:"

def dict_factory(cursor, row): # Stolen from documentation
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class SqliteInterface(DBInterface):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connect()

    @property
    def cursor(self):
        return self._cursor

    # Static Methods

    @classmethod
    def _create_filter_query(cls, filter):
        assert isinstance(filter, dict)
        if not filter:
            return "", {}
        for key in filter.keys():
            if not isinstance(filter[key], (list, tuple)) or len(filter[key]) != 2:
                filter[key] = ["=", filter[key]]
            elif isinstance(filter[key], (list, tuple)) and len(filter[key]) == 2:
                if (isinstance(filter[key][0], str) and
                    filter[key][0].upper() in ("=", "!=", "<=", ">=", "<", ">", "LIKE")):
                    if filter[key][0].upper() == "LIKE":
                        filter[key][0] = " LIKE "
                else:
                    raise Exception("Operation not allowed (yet)")
        keys = list(filter.keys())
        string = " and ".join([key+filter[key][0]+":filter"+key+"value"+str(i) for i, key in enumerate(keys)])
        string = "WHERE {}".format(string)
        safe = {}
        for i,key in enumerate(keys):
            safe["filter"+key+"value"+str(i)] = filter[key][1]
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
                                {PRIMARY: "PRIMARY KEY",
                                str: "TEXT",
                                int: "INTEGER",
                                float: "REAL",
                                None: "NULL",
                                object: "BLOB"})
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
        assert len(fields) == len(values)
        safe = {}
        fields_str = ", ".join([key for key in fields])
        values_str = ", ".join([":"+key+"value" for key in fields])
        for index, key in enumerate(fields):
            safe[key+"value"] = values[index]
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
        data = kwargs["data"]
        filter = kwargs["filter"]
        if not isinstance(filter, dict):
            filter = {}
        if "exists" in kwargs:
            exists = kwargs["exists"]
        else:
            exists = False
        sql_string = ""
        sql_safe_passing = {}
        template = {SELECT: "SELECT {fields} FROM {table} {where};",
                    INSERT: "INSERT INTO {table} ({fields}) VALUES ({values});",
                    UPDATE: "UPDATE {table} SET {pairing} {where};",
                    DELETE: "DELETE from {table} {where};",
                    CREATE_TABLE: "CREATE TABLE {exists} {table} ({pairing});",
                    DROP_TABLE: "DROP TABLE IF EXISTS {table}"}

        if method == SELECT:
            if fields and (isinstance(fields, list) or isinstance(fields, tuple)):
                field_str = ", ".join(fields)
            else:
                field_str = "*"
            where_str, sql_safe_passing = self._create_filter_query(filter)
            sql_string = template[method].format(fields=field_str,
                                                where=where_str,
                                                table=table)
        elif method == INSERT:
            fields_str, values_str, sql_safe_passing = self._create_fields_value_for_insert(fields, data)
            sql_string = template[method].format(fields=fields_str,
                                                values=values_str,
                                                table=table)
        elif method == UPDATE:
            pairing, sql_safe_passing = self._create_fields_pairing(fields, data, "=")
            where_str, safe = self._create_filter_query(filter)
            sql_string = template[method].format(pairing=pairing,
                                                 where=where_str,
                                                 table=table)
            sql_safe_passing.update(safe)
        elif method == DELETE:
            where_str, sql_safe_passing = self._create_filter_query(filter)
            sql_string = template[method].format(where=where_str, table=table)
        elif method == CREATE_TABLE:
            pairing, sql_safe_passing = self._create_fields_pairing(fields, data, " ")
            if exists:
                exists_str = "IF NOT EXISTS"
            else:
                exists_str = ""
            sql_string = template[method].format(pairing=pairing,
                                                 exists=exists_str,
                                                 table=table)
        elif method == DROP_TABLE:
            sql_string = template[method].format(table=table)

        return sql_string, sql_safe_passing
    # Connection Methods

    def connect(self):
        self._conn = sqlite3.connect(self._server)
        self._conn.row_factory = dict_factory
        self._cursor = self._conn.cursor()

    def disconnect(self):
        self._conn.close()

    # Tables operations
    def create_table(self, table, fields={}, exists=True):
        assert fields # Thay must not be void
        if isinstance (fields, dict):
            data = fields.values()
            fields = fields.keys()
        kwargs = self.sql_dict
        kwargs.update(method=CREATE_TABLE,
                      table=table,
                      fields=fields,
                      data=data,
                      exists=exists)
        sql, safe = self._create_sql_query(**kwargs)
        self.cursor.execute(sql, safe)
        self._conn.commit()

    def drop_table(self, table, database=None):
        """
        drops selected table
        """
        database, table = super().drop_table(database=database, table=table)
        sql, safe = self._create_sql_query(method=DROP_TABLE,
                                            table=table)
        self.cursor.execute(sql, safe)
        return Data(self.cursor.fetchall())

    # Executings

    def select(self, **kwargs):
        filter, database, table, fields = super().select(**kwargs)
        sql, safe = self._create_sql_query(method=SELECT,
                                            table=table,
                                            fields=fields,
                                            filter=filter)
        self.cursor.execute(sql, safe)
        return Data(self.cursor.fetchall())

    def insert(self, data, **kwargs):
        database, table, fields, values = super().insert(data, **kwargs)
        sql, safe = self._create_sql_query(method=INSERT,
                                            table=table,
                                            fields=fields,
                                            data=values)
        self.cursor.execute(sql, safe)
        self._conn.commit()

    def update(self, data, **kwargs):
        filter, database, table, fields, values = super().update(data, **kwargs)
        sql, safe = self._create_sql_query(method=UPDATE,
                                            table=table,
                                            fields=fields,
                                            data=values,
                                            filter=filter)
        self.cursor.execute(sql, safe)
        self._conn.commit()

    def delete(self, **kwargs):
        filter, database, table = super().delete(**kwargs)
        sql, safe = self._create_sql_query(method=DELETE,
                                            table=table,
                                            filter=filter)
        self.cursor.execute(sql, safe)
        self._conn.commit()
