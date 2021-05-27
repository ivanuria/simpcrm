# databases_tests.py
# Unit Testing for databases

VERSION = 0.1

import unittest
from databases.sqlite import SqliteInterface as SQLite, MEMORY
from databases.databases import Data, SELECT, INSERT, UPDATE, DELETE, CREATE_TABLE, DROP_TABLE
from entities.defaults import get_entity, get_entities, persistent, install_persistency
from entities.entities import Entity
from entities.fields import Field, Fields
from sqlite3 import Error

class v1_Fields(unittest.TestCase):
    def setUp(self):
        self.db = SQLite(server=MEMORY)
        self.db.connect()

    def tearDown(self):
        self.db.disconnect()

    def test_Field(self):
        field = Field(self.db, "ninini", "foo", "bar", description="testing")
        self.assertEqual (field.name, "foo")
        self.assertEqual (field.definition, "bar")
        self.assertEqual (field.description, "testing")

    def test_Fields(self):
        fields_def = {"foo": str, "bar": int}
        fields = Fields(self.db, "ninini", {"foo": str, "bar": int})
        self.assertEqual(fields.table,"ninini")
        self.assertEqual(dict(fields), dict(map(lambda x: (x, Field(self.db, "ninini", x, fields_def[x])), fields_def)))
        self.assertEqual(fields["foo"].name, "foo")
        self.assertEqual(fields["bar"].name, "bar")
        self.assertEqual(fields["foo"].definition, str)
        self.assertEqual(fields["bar"].definition, int)
        self.assertEqual(fields["foo"].description, "")
        self.assertEqual(fields["bar"].description, "")

class v1_Entity_setup(unittest.TestCase):
    def setUp(self):
        self.db = SQLite(server=MEMORY)
        self.db.connect()
        self.entity = Entity(self.db, "ninini", "ninini", {"foo": str, "bar": int}, "Test entity")
        self.entity.install()

    def tearDown(self):
        self.db.disconnect()

    def test_Entity_definition(self):
        self.assertEqual(self.entity.name, "ninini")
        self.assertEqual(self.entity.table, "ninini")
        self.assertEqual(self.entity.database, self.db)
        self.assertEqual(self.entity.fields, Fields(self.db, "ninini", {"foo": str, "bar": int}))
        self.assertEqual(self.entity.parent, None)
        self.assertEqual(self.entity.parent_field, "")

    def test_Entity_insert_get(self):
        self.entity.install()
        self.entity.insert({"foo": "Hola", "bar": 10})
        self.entity.insert({"foo": "Adios", "bar": 12})
        self.assertEqual(self.entity.get({"foo": "Hola"}), [{"id":1, "foo": "Hola", "bar": 10}])
        self.assertEqual(self.entity.get({"foo": "Adios"}), [{"id":2, "foo": "Adios", "bar": 12}])

    def test_Entity_replace(self):
        self.entity.insert({"foo": "Hola", "bar": 10})
        self.entity.insert({"foo": "Adios", "bar": 12})
        self.entity.replace({"id": 2}, {"bar": 15})
        self.assertEqual(self.entity.get({"foo": "Hola"}), [{"id":1, "foo": "Hola", "bar": 10}])
        self.assertEqual(self.entity.get({"foo": "Adios"}), [{"id":2, "foo": "Adios", "bar": 15}])

    def test_Entity_delete(self):
        self.entity.insert({"foo": "Hola", "bar": 10})
        self.entity.insert({"foo": "Adios", "bar": 12})
        self.entity.delete({"id": 2})
        self.assertEqual(self.entity.get({}), [{"id":1, "foo": "Hola", "bar": 10}])

class v1_defaults(unittest.TestCase):
    def setUp(self):
        self.db = SQLite(server=MEMORY)
        self.db.connect()
        install_persistency(self.db)
        self.entity = Entity(self.db, "ninini", "ninini", {"foo": str, "bar": int}, "Test entity")
        self.entity.install()
        self.entity.insert({"foo": "Hola", "bar": 10})
        self.entity.insert({"foo": "Adios", "bar": 12})

    def tearDown(self):
        self.db.disconnect()

    def test_get_entity(self):
        entity = get_entity(self.db, "ninini")
        self.assertEqual(entity.get({"foo": "Hola"}), [{"id":1, "foo": "Hola", "bar": 10}])
