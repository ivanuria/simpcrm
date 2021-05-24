# Fields definitions

class Field:
    def __init__(self, name, definition, description=""):
        self._name = name
        self._definition = definition
        self.description = description

    @property
    def name(self):
        return self._name

    @property
    def definition(self):
        return self._definition

class Fields:
    def __init__(self, table, fields):
        self._table = table
        if isinstance(fields, dict):
            self._fields = list(map(lambda x: Field(x, fields[x]), fields))
        elif isinstance(fields, list) and all([isinstance(field, Field) for field in fields]):
            self._fields = Fields

    @property
    def table(self):
        return self._table

    @property
    def fields(self):
        return self._fields
