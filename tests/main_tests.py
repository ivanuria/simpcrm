# main_tests.py
# Unit Testing for databases

VERSION = 0.1

import os
import unittest
from interface import Main, hasher
from interface.main import only_permitted
from interface.defaults import DEFAULT_USERS

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
        self.assertEqual(self.main.get_role_children("admin"), ["admin", "manager", "user", "itmanager", "ituser"])
        self.assertEqual(self.main.get_role_children("manager"), ["user"])
        self.assertEqual(self.main.get_role_children("user"), [])
        self.assertEqual(self.main.get_role_children("itmanager"), ["ituser"])
        self.assertEqual(self.main.get_role_children("ituser"), [])

    def test_check_permitted_roles(self):
        roles = ["admin", "manager", "user", "itmanager", "ituser"]
        self.assertEqual(self.main.check_permitted_roles("admin", roles), roles)
        self.assertEqual(self.main.check_permitted_roles("opm001", roles), ["user"])
        self.assertEqual(self.main.check_permitted_roles("op001", roles), [])
        self.assertEqual(self.main.check_permitted_roles("op002", roles), [])
        self.assertEqual(self.main.check_permitted_roles("itm001", roles), ["ituser"])
        self.assertEqual(self.main.check_permitted_roles("it001", roles), [])
        self.assertEqual(self.main.check_permitted_roles("it002", roles), [])

    def test_only_permitted(self):
        def datest(*args, **kwargs):
            return True
        test_dict = {"admin": True,
                     "opm001": True,
                     "op001": False,
                     "op002": False,
                     "itm001": True,
                     "it001": False,
                     "it002": False}
        for user in test_dict:
            if user == "admin":
                token = self.token
            else:
                for i in DEFAULT_USERS:
                    if i["id"] == user:
                        token = self.main.login(user, i["token"])
            value = test_dict[user]
            for table in ["__users", "__roles", "__permissions"]:
                for perm in ["r", "w"]:
                    with self.subTest(user=user, value=value, table=table, perm=perm, token=token):
                        if value is True:
                            self.assertTrue(only_permitted(table=table, op=perm)
                                                          (datest) #only_permitted_decorator
                                                          (self, user=user, token=token)) #only_permitted_wrapper
                        else:
                            self.assertFalse(only_permitted(table=table, op=perm)
                                                           (datest) #only_permitted_decorator
                                                           (self, user=user, token=token)) #only_permitted_wrapper
