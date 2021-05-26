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
        fields = Fields(self.db, "ninini", {"foo": str, "bar": int})
        self.assertEqual(fields.table,"ninini")
        self.assertEqual(fields.fields, list(map(lambda x: Field(x, fields[x]), fields)))
        self.assertEqual(fields.fields[0].name, "foo")
        self.assertEqual(fields.fields[1].name, "bar")
        self.assertEqual(fields.fields[0].desfinition, str)
        self.assertEqual(fields.fields[1].desfinition, int)
        self.assertEqual(fields.fields[0].description, "")
        self.assertEqual(fields.fields[1].description, "")
