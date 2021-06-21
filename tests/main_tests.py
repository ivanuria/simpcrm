# main_tests.py
# Unit Testing for databases

VERSION = 0.1

import unittest
from interface import Main

class v1_Main(unittest.TestCase):
    def setUp(self):
        self.main = Main("./test_main_config.ini")

    def test_installed(self):
        self.assertTrue(self.main.installed)
