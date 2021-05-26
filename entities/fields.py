# Fields definitions
from collections import defaultdict

class Field:
    persistent = defaultdict(lambda: defaultdict(dict))
    def __new__(cls, database, table, name, definition, description):
        if (database in cls.persistent and
            table in cls.persistent[database] and
            name in cls.persistent[database][table]):
            return cls.persistent[database][table][name]
        else:
            return super().__new__(cls)

    def __init__(self, database, table, name, definition, description=""):
        self._name = name
        self._definition = definition
        self.description = description
        self._table = table
        self.persistent[database][table][name] = self

    @property
    def name(self):
        return self._name

    @property
    def definition(self):
        return self._definition

    @property
    def table(self):
        return self._table

class Fields(dict):
    def __init__(self, database, table, fields):
        super().__init__(self)
        if isinstance(fields, dict):
            self._fields = list(map(lambda x: Field(table, x, fields[x]), fields))
        elif isinstance(fields, list) and all([isinstance(field, Field) for field in fields]):
            self._fields = Fields
        for field in self._fields:
            self[field.name] = field
        self._table = table

    @property
    def table(self):
        return self._table

    @property
    def fields(self):
        return self._fields

    def __getitem__(self, key):
        return super().__getitem__(key).definition
