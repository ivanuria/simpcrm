# Fields definitions
from collections import defaultdict

class Field:
    persistent = defaultdict(lambda: defaultdict(dict)) #TODO save directly as Fields
    def __new__(cls, database, table, name, definition, description=""):
        if (database in cls.persistent and
            table in cls.persistent[database] and
            name in cls.persistent[database][table]):
            if cls.persistent[database][table][name].definition == definition:
                return cls.persistent[database][table][name]
            else:
                raise Exception ("field previously instantiated")
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
    persistent = defaultdict(dict)
    def __new__(cls, database, table, fields):
        if database in cls.persistent and table in cls.persistent[database]:
            print("ALERT: fields will not be checked (yet)") #TODO
            return cls.persistent[database][table]
        else:
            return super().__new__(cls)

    def __init__(self, database, table, fields):
        super().__init__(self)
        self._table = table
        self._database = database
        self._fields = Field.persistent[database][table] #Comes from Field instantation
        if isinstance(fields, dict) and fields != self._fields:
            list(map(lambda x: Field(database, table, x, fields[x]), fields))
        """elif isinstance(fields, list) and all([isinstance(field, Field) for field in fields]):
            self._fields = Fields
        for field in self._fields:
            self[field.name] = field
        self._table = table"""#absurd now

    @property
    def table(self):
        return self._table

    @property
    def fields(self):
        return self._fields

    def __getitem__(self, key):
        return self._fields[key].definition
