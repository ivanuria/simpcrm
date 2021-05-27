# Fields definitions
from collections import defaultdict

class Field:
    #persistent = defaultdict(lambda: defaultdict(dict)) #Moved to Fields
    def __new__(cls, database, table, name, definition, description=""):
        if not table in Fields.persistent[database]:
            Fields(database, table, name, {})
        if (database in Fields.persistent and
            table in Fields.persistent[database] and
            name in Fields.persistent[database][table]):
            if Fields.persistent[database][table][name].definition == definition:
                return Fields.persistent[database][table][name]
            else:
                raise Exception ("field {} previously instantiated: {} Vs {}".format(name, definition, Fields.persistent[database][table][name].definition))
        else:
            return super().__new__(cls)

    def __init__(self, database, table, name, definition, description=""):
        self._name = name
        self._definition = definition
        self.description = description
        self._table = table
        Fields.persistent[database][table][name] = self

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
    def __new__(cls, database, table, fields, installed=False):
        if database in cls.persistent and table in cls.persistent[database]:
            return cls.persistent[database][table]
        else:
            return super().__new__(cls)

    def __init__(self, database, table, fields, installed=False):
        super().__init__(self)
        self.persistent[database][table] = self
        self._table = table
        self._database = database
        self._installed = installed # To know whether or not alter table
        if isinstance(fields, dict):
            list(map(lambda x: Field(database, table, x, fields[x]), fields))

    @property
    def table(self):
        return self._table

    @property
    def fields(self):
        return dict(self)

    def values(self): #Let's override this
        return [item.definition for item in super().values()]

    def set_installed(self):
        self._installed = True
