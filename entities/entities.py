# Entities files

from ..databases.database import DBInterface
from ..databases.sqlite import SqliteInterface
from collections import defaultdict


def Entity:
    persist = defaultdict(dict)
    # A dictionary with an entity by database. Why? Suddenly my intuition sais I must do this

    def __new__(cls, name, table, fields, description, database, parent=None, parent_field=""):
        if table in cls.persist[database]:
            self = cls.persist[database][table]
            if any(name!=self.name,
                   fields!=self.fields_orig,
                   parent!=self.parent,
                   parent_field!=self.parent_field):
                raise Exception("Entity already defined")
            else:
                return self
        else:
            return super().__new__(cls)

    def __init__(self, name, table, fields, description, database, parent=None, parent_field=""):
        assert isinstance(database, DBInterface)
        assert table not in Entity[database]
        if not table in Entity.persist:
            self.install()
            Entity.persist[database][table] = self
            self._name = name
            self._table = table
            self.description = description
            self._fields_orig = fields
            self._fields = Fields(fields)
            self._database = database
            self._parent = parent
            self._parent_field = parent_field

    #Properties
    @property
    def database(self):
        return self._database

    @property
    def fields(self):
        return self._fields

    @property
    def name(self):
        return self._name

    @property
    def parent(self):
        return self._parent

    @property
    def parent_field(self):
        return self._parent_field

    @property
    def table(self):
        return self._table

    #Methods
    def delete(self, filter):
        self.database.delete(filter=filter, table=table)

    def get(self, filter):
        return self.database.select(filter=filter, table=self.table)

    def insert(self, data):
        self.database.insert(data, table=self.table)

    def install(self):
        self.database.create_table(self.table, self.fields, exists=True)

    def replace(self, filter, data):
        self.database.update(self, data, filter=filter, table=table)

    def set_database(self, database):
        assert isinstance(database, DBInterface)
        self._database = database
