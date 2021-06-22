# main_tests.py
# Unit Testing for databases

VERSION = 0.1

import os
import unittest
from interface import Main, hasher

class v1_Main(unittest.TestCase):
    def setUp(self):
        try:
            self.main = Main(configdbfile="tests\\test_main_config.ini")
            self.user, self.name, self.hash = "admin", "Iván", hasher("testingPassword123")
            self.main.install(self.user, self.name, self.hash)
            self.token = self.main.login(self.user, self.hash) #TODO: hash password must be dynamic
        except:
            self.tearDown()
            raise

    def tearDown(self):
        self.main.close()
        os.remove("tests\\test.db")

    def test_installed(self):
        try:
            self.assertTrue(self.main.installed)
        except AssertionError:
            self.tearDown()
            raise
        for i in ["__users", "__roles", "__permissions", "__simpcrm_main"]:
            self.assertTrue(i in self.main.entities)

    def test_load(self):
        try:
            main = Main(configdbfile="tests\\test_main_config.ini")
            main.load()
            self.assertTrue(main.installed)
            for i in ["__users", "__roles", "__permissions", "__simpcrm_main"]:
                self.assertTrue(i in self.main.entities)
        finally:
            main.close()
            del(main)

    def test_login(self):
        self.assertTrue(self.main.logged(self.user, self.token))

    def test_get_role_children(self):
        self.assertEqual(self.main.get_role_children("admin"), ["admin", "user"])
