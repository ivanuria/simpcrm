# databases_tests.py
# Unit Testing for databases

VERSION = 0.1

import unittest
from databases.sqlite import SqliteInterface as SQLite
from databases.databases import Data
from sqlite3 import Error

class v1_Databases_sqlite(unittest.TestCase):
    def setUp(self):
        self.db = SQLite(server=databases.sqlite.MEMORY)
        self.db.connect()
        self.cursor = self.db.__cursor
        self.cursor.execute("""Create table customers
            (id integer primary key, name text, phone text, age integer)""")
        self.cursor.execute(""" insert into customers (name, phone, age)
            values (:name, :phone, :age)""",
            {"name": "María", "age": 49, "phone": "+34666777888"})
        self.cursor.commit()

    def tearDown(self):
        self.db.disconnect()

    def test__create_filter_query(self):
        self.assertEqual(self.db._create_filter_query({}), ("", {}))
        self.assertEqual(self.db._create_filter_query(["name", "age"], ["María", 26],
                        ("WHERE :filter_name_key = :filter_name_value and :filter_age_key = :filter_age_value",
                        {"filter_name_key": "name", "filter_name_value": "María",
                        "filter_age_key": "age", "filter_age_value": 26})))

    def test__create_fields_pairing(self):
        self.assertEqual(self.db._create_fields_pairing(["name", "age"], ["María", 26],
                         (":name_key :name_value, :age_key :age_value",
                         {"name_key": "name", "name_value": "María",
                         "age_key": "age", "age_value": 26})))
        self.assertEqual(self.db._create_fields_pairing(["name", "age"], ["María", 26],
                         (":name_key=:name_value, :age_key=:age_value",
                         {"name_key": "name", "name_value": "María",
                         "age_key": "age", "age_value": 26})))

    def test__create_fields_value_for_insert(self):
        self.assertEqual(self.db._create_fields_value_for_insert(), ["name", "age"], ["María", 26],
                        (":name_key, :age_key", ":name_value, :age_value", {"name_key": "name", "name_value": "María",
                        "age_key": "age", "age_value": 26}))

    def test_set_filter(self):
        self.db.set_filter({"name": "María"})
        self.assertEqual(self.db.filter, """where name = 'María'""")
        self.db.set_filter({"name": "María", "age": 49})
        self.assertEqual(self.db.filter,
            """where name = 'María' and age = 49 """)

    def test_set_table(self):
        self.db.set_table("customers")
        self.assertEqual(self.db.table, "customers")

    def test_select(self):
        self.db.set_table("customers")
        self.assertEqual(self.db.select(filter={"name": "María"}),
            databases.Data({"id": 1, "name": "María", "age": 49, "phone": "+34666777888"}))

    def test_insert(self):
        self.db.set_table("customers")
        self.db.insert(data={"name": "José", "age": 33, "phone": "+34777888999"})
        self.assertEqual(self.db.select(filter={"name": "José"}),
            databases.Data({"id": 2, "name": "José", "age": 33, "phone": "+34777888999"}))

    def test_select_many(self):
        self.db.set_table("customers")
        self.assertEqual(self.db.select(),
            databases.Data({"id": 1, "name": "María", "age": 49, "phone": "+34666777888"},
            {"id": 2, "name": "José", "age": 33, "phone": "+34777888999"}))

    def test_update(self):
        self.db.set_table("customers")
        self.db.update({"name": "María"}, filter={"age": 25})
        self.assertEqual(self.db.select(filter={"name": "María"}),
            databases.Data({"id": 1, "name": "María", "age": 25, "phone": "+34666777888"}))

    def test_delete(self):
        self.db.set_table("customers")
        self.db.delete(filter={"name": "María"})
        self.assertEqual(self.db.select(),
            databases.Data({"id": 2, "name": "José", "age": 33, "phone": "+34777888999"}))

    def test_create_table(self):
        self.db.create_table("hell", {"name": "text", "love": "integer"})
        self.db.set_table("hell")
        self.db.insert({"name": "hola", "love": 2})
        self.assertEqual(self.sb.select(filter={"name": "hola"}),
                        Data({"name": "hola", "love": 2}))

    def test_drop_table(self):
        self.db.set_table("hell")
        self.db.drop_table()
        with assertRaises(Error):
            self.db.insert({"name": "halo", "love": 10})


if __name__ == '__main__':
    unittest.main()
