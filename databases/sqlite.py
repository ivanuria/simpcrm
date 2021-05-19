# sqlite.py file inheriting from databases.py

import sqlite3
from databases import Data, DBInterface
from databases import SELECT, INSERT, UPDATE, REMOVE, CREATE_TABLE

class SqliteInterface(DBInterface):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connect()

    @property
    def cursor(self):
        return self._cursor

    # Static Methods

    def _create_filter_query(cls, filter):
        assert isinstance(filter, dict)
        string = " and ".join([":filter_"+key+"_key = :filter_"+key+"_value" for key in filter])
        string = "WHERE {}".format(string)
        safe = {}
        for key in filter:
            safe["filter_"+key+"_key"] = key
            safe["filter_"+key+"_value"] = filter[key]
        return string, safe

    def _create_fields_pairing(cls, fields, joiner=" "):
        fields = list(set(fields))
        assert len(fields) == len(data)
        pairs = zip(fields, data)
        pairing = ", ".join([joiner.join((":"+item[0]+"_key", ":"+item[0]+"_value") for item in pairs])
        for key, value in pairs:
            sql_safe_passing[key+"_key"] = key
            sql_safe_passing[key+"_value"] = value
        return pairing, sql_safe_passing

    def _create_fields_value_for_insert(cls, fields, values):
        assert len(fields) == len(values)
        safe = {}
        fields_str = ", ".join([":"+key+"_key" for key in fields])
        values_str = ", ".join([":"+key+"_value" for key in fields])
        for index, key in enumerate(fields):
            safe[key+"_key"] = key
            safe[key+"_value"] = values[index]
        return fields_str, values_str, safe

    def _create_sql_query(cls, **kwargs):
        cls._create_sql_query(**kwargs)
        table = kwargs["table"]
        method = kwargs["method"]
        fields = kwargs["fields"]
        data = kwargs["data"]
        filter = kwargs["filter"]
        if "exists" in kwargs:
            exists = kwargs["exists"]
        else:
            exists = False
        sql_string = ""
        sql_safe_passing = {}
        template = {SELECT: "SELECT {fields} FROM :table {where}",
                    INSERT: "INSERT INTO :table ({fields}) ({values})",
                    UPDATE: "UPDATE :table SET {pairing} {where}",
                    DELETE: "DELETE from :table {where}",
                    CREATE_TABLE: "CREATE TABLE {exists} :table ({pairing})"}

        if method == SELECT:
            field_str = ", ".join(fields)
            where_str, sql_safe_passing = cls._create_filter_query(filter)
            sql_safe_passing["table"] = table
            sql_string = template[method].format(fields=field_str,
                                                where=where_str)
        elif method == INSERT:
            fields_str, values_str, sql_safe_passing = cls._create_fields_value_for_insert(fields, data)
            sql_safe_passing["table"] = table
            sql_string = template[method].format(fields=fields_str,
                                                values=values_str)
        elif method == UPDATE:
            pairing, sql_safe_passing = cls._create_fields_pairing(fields, "=")
            where_str, safe = cls._create_filter_query(filter)
            sql_string = template[method].format(pairing=pairing, where=where_str)
            sql_safe_passing["table"] = table
            sql_safe_passing.update(safe)
        elif method == DELETE:
            where_str, sql_safe_passing = cls._create_filter_query(filter)
            sql_safe_passing["table"] = table
            sql_string = template[method].format(where=where_str)
        elif method == CREATE_TABLE:
            pairing, sql_safe_passing = cls._create_fields_pairing(fields, " ")
            if exists:
                exists_str = "IF NOT EXISTS"
            else:
                exists_str = ""
            sql_string = template[method].format(pairing=pairing, exists=exists_str)
            sql_safe_passing["table"] = table

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

    # Executings

    def select(self, **kwargs):
        filter, database, table, fields = super().select(**kwargs)
        sql, safe = self._create_sql_query(method=SELECT,
                                            table=table,
                                            fields=fields)
        self.cursor.execute(sql, safe)
        self.cursor.commit()

    def insert(self, data, database=None, table=None):
        assert isinstance(data, dict)
