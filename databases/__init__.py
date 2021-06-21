# databases.py
# Databases init package

VERSION = 0.1

from .sqlite import SqliteInterface
from .databases import Data, DBInterface, DBEnums

from enum import Enum, auto

class DBTypes(Enum):
    SQLITE = auto()

def new_db_interface(dbtype, server="", user="", password="", encription="", database=""):
    types = {"sqlite": DBTypes.SQLITE,
             "sqlite3": DBTypes.SQLITE}
    if not isinstance(dbtype, DBTypes) and dbtype in types:
        dbtype = types[dbtype]
    else:
        raise TypeError("dbtype must be a DBTypes instance or a correct string from configuration")
    return {DBTypes.SQLITE: SqliteInterface(server="", user="", password="", encription="", database="")}[dbtype]
