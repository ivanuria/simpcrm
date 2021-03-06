# main_tests.py
# Unit Testing for databases

VERSION = 0.1

import os
import unittest
from entities import Entity
from interface import Main, hasher
from interface.main import only_permitted
from interface.defaults import DEFAULT_USERS, DEFAULT_PWD

class v1_Main(unittest.TestCase):
    def setUp(self):
        try:
            self.main = Main(configdbfile=os.path.join("tests", "test_main_config.ini"))
            self.user, self.name, self.pwd = "admin", "Iván", "testingPassword123"
            self.main.install(self.user, self.name, self.pwd)
            self.token = self.main.login(self.user, self.pwd) #TODO: hash password must be dynamic
        except:
            self.tearDown()
            raise

    def tearDown(self):
        self.main.close()
        os.remove(os.path.join("tests", "test.db"))

    def test_01_installed(self):
        try:
            self.assertTrue(self.main.installed)
        except AssertionError:
            self.tearDown()
            raise
        for i in ["__users", "__roles", "__permissions", "__simpcrm_main"]:
            self.assertTrue(i in self.main.entities)

    def test_02_load(self):
        try:
            main = Main(configdbfile=os.path.join("tests", "test_main_config.ini"))
            main.load()
            self.assertTrue(main.installed)
            for i in ["__users", "__roles", "__permissions", "__simpcrm_main"]:
                self.assertTrue(i in self.main.entities)
        finally:
            main.close()
            del(main)

    def test_03_login(self):
        self.assertTrue(self.main.logged(self.user, self.token))

    def test_04_get_role_children(self):
        self.assertEqual(self.main.get_role_children("admin"), ["admin", "manager", "user", "itmanager", "ituser"])
        self.assertEqual(self.main.get_role_children("manager"), ["user"])
        self.assertEqual(self.main.get_role_children("user"), [])
        self.assertEqual(self.main.get_role_children("itmanager"), ["ituser"])
        self.assertEqual(self.main.get_role_children("ituser"), [])

    def test_05_check_permitted_roles(self):
        roles = ["admin", "manager", "user", "itmanager", "ituser"]
        self.assertEqual(self.main.check_permitted_roles("admin", roles), roles)
        self.assertEqual(self.main.check_permitted_roles("opm001", roles), ["user"])
        self.assertEqual(self.main.check_permitted_roles("op001", roles), [])
        self.assertEqual(self.main.check_permitted_roles("op002", roles), [])
        self.assertEqual(self.main.check_permitted_roles("itm001", roles), ["ituser"])
        self.assertEqual(self.main.check_permitted_roles("it001", roles), [])
        self.assertEqual(self.main.check_permitted_roles("it002", roles), [])

    def test_06_only_permitted(self):
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
                        token = self.main.login(user, DEFAULT_PWD)
            value = test_dict[user]
            for table in ["__users", "__roles", "__permissions", "__entities"]:
                for perm in ["r", "w"]:
                    with self.subTest(user=user, value=value, table=table, perm=perm, token=token):
                        if value is True:
                            self.assertTrue(only_permitted(table=table, operation=perm)
                                                          (datest) #only_permitted_decorator
                                                          (self.main, user=user, token=token)) #only_permitted_wrapper
                        else:
                            with self.assertRaises(RuntimeError):
                                (only_permitted(table=table, operation=perm)
                                               (datest) #only_permitted_decorator
                                               (self.main, user=user, token=token)) #only_permitted_wrapper

    def test_07_get_self_permissions(self):
        permissions = self.main.get_self_permissions(user=self.user, token=self.token)
        self.assertEqual(permissions,
                         {"__users": {"r": True,
                                      "w": True},
                          "__roles": {"r": True,
                                      "w": True},
                          "__permissions": {"r": True,
                                            "w": True},
                          "__entities": {"r": True,
                                         "w": True}})

    def test_08_get_permited_permissions_changes(self):
        permissions = [{"entity": "__entities",
                        "operation": "r",
                        "permitted": True,
                        "__roles_id": "itmanager"},
                       {"entity": "__entities",
                        "operation": "w",
                        "permitted": True,
                        "__roles_id": "itmanager"}]
        permitted = self.main.get_permited_permissions_changes(self.user, self.token, permissions)
        self.assertEqual(permissions, permitted)

    def test_09_new_entity_add_get_replace_delete(self):
        self.main.new_entity("customers",
                             "Customers",
                             {"name": str,
                              "age": int,
                              "gender": str},
                             "Keep your customers satisfied",
                             user=self.user, token=self.token)
        for role in ["admin", "manager", "user"]:
            permissions = [{"entity": "customers", "operation": "r", "permitted": True},
                           {"entity": "customers", "operation": "w", "permitted": True}]
            self.main.modify_role(role, None, None, permissions, user=self.user, token=self.token)
        self.main.add_data("customers",
                           [{"name": "Lola",
                            "age": 23,
                            "gender": "NB"},
                            {"name": "Chelo",
                             "age": 56,
                             "gender": "Male"},
                            {"name": "Austin",
                             "age": 5,
                             "gender": "NS/NC"}],
                           user=self.user, token=self.token)
        self.assertEqual(self.main.get_data("customers", {"name": "Lola"},
                                            user=self.user, token=self.token),
                        [{"id": 1, "name": "Lola", "age": 23, "gender": "NB"}])
        self.main.replace_data("customers", {"name": "Lola"},
                               {"gender": "Male", "age": 54},
                               user=self.user, token=self.token)
        self.assertEqual(self.main.get_data("customers", {"name": "Lola"},
                                            user=self.user, token=self.token),
                        [{"id": 1, "name": "Lola", "age": 54, "gender": "Male"}])
        self.main.delete_data("customers", {"name": "Lola"},
                               user=self.user, token=self.token)
        self.assertEqual(self.main.get_data("customers", {"name": "Lola"},
                                            user=self.user, token=self.token),
                        [])

    def test_10_new_entity_add_get_replace_delete_user(self):
        self.main.new_entity("customers",
                             "Customers",
                             {"name": str,
                              "age": int,
                              "gender": str},
                             "Keep your customers satisfied",
                             user=self.user, token=self.token)
        for role in ["admin", "manager", "user"]:
            permissions = [{"entity": "customers", "operation": "r", "permitted": True},
                           {"entity": "customers", "operation": "w", "permitted": True}]
            self.main.modify_role(role, None, None, permissions, user=self.user, token=self.token)
        self.main.add_data("customers",
                           [{"name": "Lola",
                            "age": 23,
                            "gender": "NB"},
                            {"name": "Chelo",
                             "age": 56,
                             "gender": "Male"},
                            {"name": "Austin",
                             "age": 5,
                             "gender": "NS/NC"}],
                           user=self.user, token=self.token)
        for u in DEFAULT_USERS:
            user = u["id"]
            if user == ["opm002", "op001", "op002"]:
                token = self.main.login(user, DEFAULT_PWD)
                self.assertEqual(self.main.get_data("customers", {"name": "Lola"},
                                                    user=user, token=token),
                                [{"id": 1, "name": "Lola", "age": 23, "gender": "NB"}])
                self.main.replace_data("customers", {"name": "Lola"},
                                       {"gender": "Male", "age": 54},
                                       user=user, token=token)
                self.assertEqual(self.main.get_data("customers", {"name": "Lola"},
                                                    user=user, token=token),
                                [{"id": 1, "name": "Lola", "age": 54, "gender": "Male"}])
                self.main.delete_data("customers", {"name": "Lola"},
                                       user=user, token=token)
                self.assertEqual(self.main.get_data("customers", {"name": "Lola"},
                                                    user=user, token=token),
                                [])
                break
            else:
                continue

    def test_11_new_entity_add_get_replace_delete_user(self):
        self.main.new_entity("customers",
                             "Customers",
                             {"name": str,
                              "age": int,
                              "gender": str},
                             "Keep your customers satisfied",
                             user=self.user, token=self.token)
        for role in ["admin", "manager", "user"]:
            permissions = [{"entity": "customers", "operation": "r", "permitted": True},
                           {"entity": "customers", "operation": "w", "permitted": True}]
            self.main.modify_role(role, None, None, permissions, user=self.user, token=self.token)
        self.main.add_data("customers",
                           [{"name": "Lola",
                            "age": 23,
                            "gender": "NB"},
                            {"name": "Chelo",
                             "age": 56,
                             "gender": "Male"},
                            {"name": "Austin",
                             "age": 5,
                             "gender": "NS/NC"}],
                           user=self.user, token=self.token)
        for u in DEFAULT_USERS:
            user = u["id"]
            if user in ["itm002", "it001", "it002"]:
                token = self.main.login(user, DEFAULT_PWD)
                with self.assertRaises(RuntimeError):
                    self.assertEqual(self.main.get_data("customers", {"name": "Lola"},
                                                        user=user, token=token),
                                    [{"id": 1, "name": "Lola", "age": 23, "gender": "NB"}])
                with self.assertRaises(RuntimeError):
                    self.main.replace_data("customers", {"name": "Lola"},
                                           {"gender": "Male", "age": 54},
                                           user=user, token=token)
                with self.assertRaises(RuntimeError):
                    self.assertEqual(self.main.get_data("customers", {"name": "Lola"},
                                                        user=user, token=token),
                                    [{"id": 1, "name": "Lola", "age": 54, "gender": "Male"}])
                with self.assertRaises(RuntimeError):
                    self.main.delete_data("customers", {"name": "Lola"},
                                           user=user, token=token)
                with self.assertRaises(RuntimeError):
                    self.assertEqual(self.main.get_data("customers", {"name": "Lola"},
                                                        user=user, token=token),
                                    [])
                break
            else:
                continue
