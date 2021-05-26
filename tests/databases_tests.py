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

    def test__create_filter_query_default(self):
        self.assertEqual(SQLite._create_filter_query({}), ("", {}))
        self.assertEqual(SQLite._create_filter_query({"name": "María", "age": 26}),
                        ("WHERE name=:filternamevalue0 and age=:filteragevalue1",
                        {"filternamevalue0": "María",
                         "filteragevalue1": 26}))

    def test__create_filter_query(self):
        self.assertEqual(SQLite._create_filter_query({"name": ["=", "María"],
                                                      "age": ["!=", 26],
                                                      "channels": ["<", 4],
                                                      "inputs": [">", 4],
                                                      "outputs": [">=", 8],
                                                      "volume": ["<=", 16],
                                                      "gain": ["like", None]}),
                        ("WHERE name=:filternamevalue0 and age!=:filteragevalue1 and channels<:filterchannelsvalue2 and inputs>:filterinputsvalue3 and outputs>=:filteroutputsvalue4 and volume<=:filtervolumevalue5 and gain LIKE :filtergainvalue6",
                        {"filternamevalue0": "María",
                         "filteragevalue1": 26,
                         "filterchannelsvalue2": 4,
                         "filterinputsvalue3": 4,
                         "filteroutputsvalue4": 8,
                         "filtervolumevalue5": 16,
                         "filtergainvalue6": None}))

    def test__create_fields_pairing(self):
        self.assertEqual(SQLite._create_fields_pairing(["name", "age"], [str, int]),
                         ("name TEXT, age INTEGER",
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
                        ("SELECT a, b FROM foo WHERE a=:filteravalue0 and b=:filterbvalue1;",
                        {"filteravalue0": 1, "filterbvalue1": "dos"}))

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
                        ("UPDATE foo SET a=:avalue, b=:bvalue WHERE a=:filteravalue0 and b=:filterbvalue1;",
                        {"avalue": 1, "bvalue": "dos", "filteravalue0": 1, "filterbvalue1": "dos"}))

    def test__create_sql_query_delete(self):
        self.assertEqual(self.db._create_sql_query(table="foo",
                                                   filter={"a": 1, "b": "dos"},
                                                   method=DELETE),
                        ("DELETE from foo WHERE a=:filteravalue0 and b=:filterbvalue1;",
                        {"filteravalue0": 1, "filterbvalue1": "dos"}))

    def test__create_sql_query_create_table(self):
        self.assertEqual(self.db._create_sql_query(table="foo",
                                                   fields=["a", "b"],
                                                   data=[int, str],
                                                   method=CREATE_TABLE,
                                                   exists=True),
                        ("CREATE TABLE IF NOT EXISTS foo (a INTEGER, b TEXT);",
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
        self.db.insert(data={"name": "José", "age": 33, "phone": "+34777888999"})
        self.assertEqual(self.db.select(),
            Data([{"id": 1, "name": "María", "age": 49, "phone": "+34666777888"},
            {"id": 2, "name": "José", "age": 33, "phone": "+34777888999"}]))

    def test_update(self):
        self.db.set_table("customers")
        self.db.update({"age": 25}, filter={"name": "María"})
        self.assertEqual(self.db.select(filter={"name": "María"}),
            Data({"id": 1, "name": "María", "age": 25, "phone": "+34666777888"}))

    def test_delete(self):
        self.db.set_table("customers")
        self.db.insert(data={"name": "José", "age": 33, "phone": "+34777888999"})
        self.db.delete(filter={"name": "María"})
        self.assertEqual(self.db.select(),
            Data({"id": 2, "name": "José", "age": 33, "phone": "+34777888999"}))

    def test_create_table(self):
        self.db.create_table("hell", {"name": str, "love": int})
        self.db.set_table("hell")
        self.db.insert({"name": "hola", "love": 2})
        self.assertEqual(self.db.select(filter={"name": "hola"}),
                        Data({"name": "hola", "love": 2}))

    def test_drop_table(self):
        self.db.drop_table("hell")
        with self.assertRaises(Error):
            self.db.set_table("hell")
            self.db.insert({"name": "halo", "love": 10})


if __name__ == '__main__':
    unittest.main()
