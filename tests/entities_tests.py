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
        pass

    def tearDown(self):
        pass

    def test_Field(self):
        field = Field("foo", "bar", description="testing")
        assertEqual (field.name, "foo")
        assertEqual (field.definition, "bar")
        assertEqual (field.description, "testing")

    def test_Fields(self):
        fields = Fields("ninini", {"foo": str, "bar": int})
        assertEqual(fields.table,"ninini")
        assertEqual(fields.fields, fields)
        assertEqual(fields.fields[0].name, "foo")
        assertEqual(fields.fields[1].name, "bar")
        assertEqual(fields.fields[0].desfinition, str)
        assertEqual(fields.fields[1].desfinition, int)
        assertEqual(fields.fields[0].description, "")
        assertEqual(fields.fields[1].description, "")
