# databases_tests.py
# Unit Testing for databases

VERSION = 0.1

import unittesting
import databases

class v1_Databases_sqlite(unittest.TestCase):
    def setUp(self):
        self.db = databases.SQLite(server=databases.sqlite.MEMORY)
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

    def test_set_filter(self):
        self.db.set_filter({"name": "María"})
        self.assertEqual(self.db.filter, """where name = 'María'""")
        self.db.set_filter({"name": "María", "age": 49})
        self.assertEqual(self.db.filter,
            """where name = 'María' and age = 49 """)

    def test_select(self):
        self.assertEqual(self.db.select({"name": "María"}),
            databases.Data({"id": 1, "name": "María", "age": 49, "phone": "+34666777888"}))

    def test_insert(self):
        self.db.insert({"name": "José", "age": 33, "phone": "+34777888999"})
        self.assertEqual(self.db.select({"name": "José"}),
            databases.Data({"id": 2, "name": "José", "age": 33, "phone": "+34777888999"}))

    def test_select_many(self):
        self.assertEqual(self.db.select(),
            databases.Data([{"id": 1, "name": "María", "age": 49, "phone": "+34666777888"},
            {"id": 2, "name": "José", "age": 33, "phone": "+34777888999"}))

    def test_update(self):
        self.db.update({"name": "María"}, filter={"age": 25})
        self.assertEqual(self.db.select({"name": "María"}),
            databases.Data({"id": 1, "name": "María", "age": 25, "phone": "+34666777888"}))

    def test_remove(self):
        self.db.remove({"name": "María"})
        self.assertEqual(self.db.select(),
            databases.Data([{"id": 2, "name": "José", "age": 33, "phone": "+34777888999"}))


"""
    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')

    def test_isupper(self):
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)
"""
if __name__ == '__main__':
    unittest.main()
