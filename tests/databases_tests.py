# databases_tests.py
# Unit Testing for databases

VERSION = 0.1

import unittest
from databases.sqlite import SqliteInterface as SQLite, MEMORY
from databases.databases import Data, SELECT, INSERT, UPDATE, DELETE, CREATE_TABLE, DROP_TABLE
from sqlite3 import Error

class v1_Databases_sqlite(unittest.TestCase):
    def setUp(self):
        self.db = SQLite(server=MEMORY)
        self.db.connect()
        self.cursor = self.db._cursor
        self.conn = self.db._conn
        self.cursor.execute("""Create table customers
            (id integer primary key, name text, phone text, age integer)""")
        self.cursor.execute(""" insert into customers (name, phone, age)
            values (:name, :phone, :age)""",
            {"name": "María", "age": 49, "phone": "+34666777888"})
        self.conn.commit()

    def tearDown(self):
        self.db.disconnect()

    def test__create_filter_query(self):
        self.assertEqual(SQLite._create_filter_query({}), ("", {}))
        self.assertEqual(SQLite._create_filter_query({"name": "María", "age": 26}),
                        ("WHERE name = :filternamevalue and age = :filteragevalue",
                        {"filternamevalue": "María",
                         "filteragevalue": 26}))

    def test__create_fields_pairing(self):
        self.assertEqual(SQLite._create_fields_pairing(["name", "age"], ["text", "integer"]),
                         ("name text, age integer",
                         {}))
        self.assertEqual(SQLite._create_fields_pairing(["name", "age"], ["María", 26], joiner="="),
                         ("name=:namevalue, age=:agevalue",
                         {"namevalue": "María",
                          "agevalue": 26}))

    def test__create_fields_value_for_insert(self):
        self.assertEqual(SQLite._create_fields_value_for_insert(["name", "age"], ["María", 26]),
                        ("name, age", ":namevalue, :agevalue",
                        {"namevalue": "María",
                         "agevalue": 26}))

    def test__create_sql_query_select(self):
        self.assertEqual(self.db._create_sql_query(table="foo",
                                                   fields=["a", "b"],
                                                   filter={"a": 1, "b": "dos"}),
                        ("SELECT a, b FROM foo WHERE a = :filteravalue and b = :filterbvalue;",
                        {"filteravalue": 1, "filterbvalue": "dos"}))

    def test__create_sql_query_insert(self):
        self.assertEqual(self.db._create_sql_query(table="foo",
                                                   fields=["a", "b"],
                                                   data=[1, "dos"],
                                                   method=INSERT),
                        ("INSERT INTO foo (a, b) VALUES (:avalue, :bvalue);",
                        {"avalue": 1, "bvalue": "dos"}))

    def test__create_sql_query_update(self):
        self.assertEqual(self.db._create_sql_query(table="foo",
                                                   fields=["a", "b"],
                                                   data=[1, "dos"],
                                                   filter={"a": 1, "b": "dos"},
                                                   method=UPDATE),
                        ("UPDATE foo SET a=:avalue, b=:bvalue WHERE a = :filteravalue and b = :filterbvalue;",
                        {"avalue": 1, "bvalue": "dos", "filteravalue": 1, "filterbvalue": "dos"}))

    def test__create_sql_query_delete(self):
        self.assertEqual(self.db._create_sql_query(table="foo",
                                                   filter={"a": 1, "b": "dos"},
                                                   method=DELETE),
                        ("DELETE from foo WHERE a = :filteravalue and b = :filterbvalue;",
                        {"filteravalue": 1, "filterbvalue": "dos"}))

    def test__create_sql_query_create_table(self):
        self.assertEqual(self.db._create_sql_query(table="foo",
                                                   fields=["a", "b"],
                                                   data=["integer", "text"],
                                                   method=CREATE_TABLE,
                                                   exists=True),
                        ("CREATE TABLE IF NOT EXISTS foo (a integer, b text);",
                        {}))

    def test__create_sql_query_drop_table(self):
        self.assertEqual(self.db._create_sql_query(table="foo",
                                                   method=DROP_TABLE),
                        ("DROP TABLE IF EXISTS foo",
                        {}))

    def test_set_filter(self):
        self.db.set_filter({"name": "María"})
        self.assertEqual(self.db.filter, {"name": "María"})
        self.db.set_filter({"name": "María", "age": 49})
        self.assertEqual(self.db.filter,
            {"name": "María", "age": 49})

    def test_set_table(self):
        self.db.set_table("customers")
        self.assertEqual(self.db.table, "customers")

    def test_select(self):
        self.db.set_table("customers")
        self.assertEqual(self.db.select(filter={"name": "María"}),
            Data({"id": 1, "name": "María", "age": 49, "phone": "+34666777888"}))

    def test_insert(self):
        self.db.set_table("customers")
        self.db.insert(data={"name": "José", "age": 33, "phone": "+34777888999"})
        self.assertEqual(self.db.select(filter={"name": "José"}),
            Data({"id": 2, "name": "José", "age": 33, "phone": "+34777888999"}))

    def test_select_many(self):
        self.db.set_table("customers")
        self.assertEqual(self.db.select(),
            Data({"id": 1, "name": "María", "age": 49, "phone": "+34666777888"},
            {"id": 2, "name": "José", "age": 33, "phone": "+34777888999"}))

    def test_update(self):
        self.db.set_table("customers")
        self.db.update({"name": "María"}, filter={"age": 25})
        self.assertEqual(self.db.select(filter={"name": "María"}),
            Data({"id": 1, "name": "María", "age": 25, "phone": "+34666777888"}))

    def test_delete(self):
        self.db.set_table("customers")
        self.db.delete(filter={"name": "María"})
        self.assertEqual(self.db.select(),
            Data({"id": 2, "name": "José", "age": 33, "phone": "+34777888999"}))

    def test_create_table(self):
        self.db.create_table("hell", {"name": "text", "love": "integer"})
        self.db.set_table("hell")
        self.db.insert({"name": "hola", "love": 2})
        self.assertEqual(self.db.select(filter={"name": "hola"}),
                        Data({"name": "hola", "love": 2}))

    def test_drop_table(self):
        self.db.drop_table("hell")
        with self.assertRaises(Error):
            self.db.insert({"name": "halo", "love": 10})


if __name__ == '__main__':
    unittest.main()
