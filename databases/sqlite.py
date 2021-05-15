# sqlite.py file inheriting from databases.py

import sqlite3
from databases import Data, DBInterface
from databases import SELECT, INSERT, UPDATE, REMOVE, CREATE_TABLE

class SqliteInterface(DBInterface):
    def __init__(self, *args, server=None, **kwargs):
        self._server = server
        self.connect()

    @property
    def cursor(self):
        return self._cursor

    # Static Methods

    def _create_sql_query(cls, **kwargs):
        cls._create_sql_query(**kwargs)
        table = kwargs["table"]
        method = kwargs["method"]
        fields = kwargs["fields"]
        data = kwargs["data"]
        sql_string = ""
        sql_safe_passing = {}
        template = {SELECT: "SELECT {fields} FROM :table {where}",
                    INSERT: "INSERT INTO :table {fields} {values}",
                    UPDATE: "UPDATE :table SET {pairing} {where}",
                    CREATE_TABLE: "CREATE TABLE :table ({pairing})"}

        if method == SELECT:
            pass
        elif method == INSERT:
            pass
        elif method == UPDATE:
            pass
        elif method == REMOVE:
            pass
        elif method == CREATE_TABLE:
            fields = list(set(fields))
            assert len(fields) == len(data)
            pairs = zip(fields, data)
            pairing = ", ".join([" ".join((":"+item[0]+"_key", ":"+item[0]+"_value") for item in pairs])

            sql_string = template[method].format(pairing=pairing)
            sql_safe_passing = {"table": table}
            for key, value in pairs:
                sql_safe_passing[key+"_key"] = key
                sql_safe_passing[key+"_value"] = value

        return sql_string, sql_safe_passing
    # Connection Methods

    def connect(self):
        self._conn = sqlite3.connect(self._server)
        self._cursor = self._conn.Cursor()

    def disconnect(self):
        self._conn.disconnect()

    # Tables operations
    def create_table(self, table, fields=[], data=[]):
        assert fields # Thay must not be void
        if isinstance (fields, dict):
            fields = fields.keys()
            data = fields.values()
        kwargs = self.sql_dict
        kwargs.update(method=CREATE_TABLE,
                                table=table,
                                fields=fields,
                                data=data)
        sql, safe = self._create_sql_query(**kwargs)
        self.cursor.execute(sql, safe)
        self.cursor.commit()
