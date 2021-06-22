# main_tests.py
# Unit Testing for databases

VERSION = 0.1

import hashlib
import os
import unittest
from interface import Main

def hash(pwd="testingPassword123"):
    salt = os.urandom(32)
    return hashlib.pbkdf2_hmac("sha256", pwd.encode("utf-8"), salt, 100000)
    # https://nitratine.net/blog/post/how-to-hash-passwords-in-python/

class v1_Main(unittest.TestCase):
    def setUp(self):
        try:
            self.main = Main(configdbfile="tests\\test_main_config.ini")
            self.user, self.name, self.hash = "admin", "Iván", hash()
            self.main.install(self.user, self.name, self.hash)
        except:
            try:
                os.remove("tests\\test.db")
            except:
                pass

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
        except AssertionError:
            print(main)
        finally:
            main.close()
            del(main)
