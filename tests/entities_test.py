# databases_tests.py
# Unit Testing for databases

VERSION = 0.1

import unittest
from databases.sqlite import SqliteInterface as SQLite, MEMORY
from databases.databases import Data, DBEnums
from entities.defaults import get_entity, get_entities, persistent, install_persistency
from entities.entities import Entity, TIMEOUT, set_timeout
from entities.fields import Field, Fields
from sqlite3 import Error
import threading
import asyncio
import time
import os

class v1_Fields(unittest.TestCase):
    def setUp(self):
        self.db = SQLite(database=MEMORY)
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
        self.db = SQLite(database=MEMORY)
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
        self.assertEqual(self.entity.parent, "")
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

    def test_Entity_uninstall(self):
        self.entity.uninstall()
        self.assertFalse("ninini" in Entity.persistent)
        with self.assertRaises(Error):
            cursor = self.db.cursor
            cursor.execute("SELECT * from ninini where 1")
            cursor.fetchall()


class v1_Entity_and_defaults(unittest.TestCase):
    def setUp(self):
        self.db = SQLite(database=MEMORY)
        self.db.connect()
        install_persistency(self.db)
        self.entity = Entity(self.db, "ninini", "ninini", {"foo": str, "bar": int}, "Test entity")
        self.entity.install()
        self.entity.insert({"foo": "Hola", "bar": 10})
        self.entity.insert({"foo": "Adios", "bar": 12})

    def tearDown(self):
        self.db.disconnect()

    def test_Entity_persistency(self):
        self.assertTrue("__entities" in Entity.persistent[self.db])
        self.assertTrue("__fields" in Entity.persistent[self.db])

    def test_instalation(self):
        c = self.db.cursor
        c.execute("select name, table_name, description from __entities where table_name=:n", {"n":"ninini"})
        a = c.fetchone()
        self.assertEqual(a, {"name": "ninini", "table_name": "ninini", "description": "Test entity"})

    def test_get_entity(self):
        entity = get_entity(self.db, "ninini")
        self.assertEqual(entity.get({"foo": "Hola"}), [{"id":1, "foo": "Hola", "bar": 10}])
        del(entity)
        del(Entity.persistent[self.db]["ninini"])
        entity = get_entity(self.db, "ninini")
        self.assertEqual(entity.get({"foo": "Hola"}), [{"id":1, "foo": "Hola", "bar": 10}])

    def test_new_field(self):
        self.entity.fields["kitty"] = float
        self.entity.replace({"id": 1}, {"kitty": 1.3})
        self.assertEqual(self.entity.get({"id": 1}),
                        [{"id": 1, "foo": "Hola", "bar": 10, "kitty": 1.3}])

    def test_change_field(self):
        self.entity.fields["bar"] = "kitty"
        self.assertEqual(self.entity.get({"id": 1}),
                        [{"id": 1, "foo": "Hola", "kitty": 10}])

    def test_get_primary_key(self):
        self.assertEqual(self.entity.fields.installed, True)
        self.assertEqual(self.entity.table, "ninini")
        self.assertEqual(self.entity.primary_key, "id")

    def test_get_getitem(self):
        self.assertEqual(self.entity[1], {"id": 1, "foo": "Hola", "bar": 10})
        self.assertEqual(self.entity[1:10], [{"id": 1, "foo": "Hola", "bar": 10},
                                             {"id": 2, "foo": "Adios", "bar": 12}])
        self.assertEqual(self.entity["foo": "Hola"], [{"id": 1, "foo": "Hola", "bar": 10}])

class v1_Item(unittest.TestCase):
    def setUp(self):
        self.db = SQLite(database="test.db")
        self.db.connect()
        install_persistency(self.db)
        self.loop = asyncio.new_event_loop()
        self.entity = Entity(self.db, "ninini", "ninini", {"foo": str, "bar": int}, "Test entity", loop=self.loop)
        self.entity.install()
        self.entity.insert({"foo": "Hola", "bar": 10})
        self.entity.insert({"foo": "Adios", "bar": 12})
        self.thread = threading.Thread(target=self.loop.run_forever)
        self.thread.start()
        self.hola, self.adios = self.entity[1:2]
        set_timeout(1)

    def tearDown(self):
        self.db.disconnect()
        self.entity.close()
        self.loop.call_soon_threadsafe(self.loop.stop)
        while True:
            if self.loop.is_running() is False:
                self.loop.close()
                break
            time.sleep(1)
        self.thread.join()
        os.remove("test.db")

    def test_entity_loop(self):
        self.assertEqual(self.entity._loop, self.loop)
        self.assertEqual(self.hola._loop, self.loop)

    def test_item_info(self):
        self.assertEqual(self.hola, {"id": 1, "foo": "Hola", "bar": 10})

    def test_item_setitem(self):
        self.hola["bar"] = 12
        self.assertEqual(self.hola, {"id": 1, "foo": "Hola", "bar": 12})

    def test_item_get_from_server(self):
        self.hola.close()
        self.entity.replace({"id": 1}, {"foo": "jurl jurl jurl"})
        self.hola._get_from_server()
        self.assertEqual(self.hola, {"id": 1, "foo": "jurl jurl jurl", "bar": 10})

    def test_item_automatic_update(self):
        self.assertEqual(self.hola._loop, self.loop)
        self.entity.replace({"id": 1}, {"foo": "jurl jurl jurl"})
        time.sleep(TIMEOUT+2)
        self.assertEqual(self.hola, {"id": 1, "foo": "jurl jurl jurl", "bar": 10})

    def test_item_handlers(self):
        self.x = None
        def handler(x, self=self):
            self.x = x
        self.hola.set_handler("foo", handler)
        self.entity.replace({"id": 1}, {"foo": "jurl jurl jurl"})
        time.sleep(TIMEOUT+2)
        self.assertEqual(self.x, "jurl jurl jurl")
        def testing(new, self=self):
            x = self.hola.changed_handler("foo")
            x(new)
        testing("nana")
        self.assertEqual(self.hola, {"id": 1, "foo": "nana", "bar": 10})


if __name__ == '__main__':
    unittest.main()
