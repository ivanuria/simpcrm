# Entities files

from ..databases.sqlite import SqliteInterface


def Entity:
    def __init__(self, name, table, fields, description=""):
        self._name = name
        self._table = table
        self.description = description
        self._fields = Fields(fields)
        self.install(self)

    
