# databases.py
# Databases init package

VERSION = 0.1

from .sqlite import SqliteInterface
from .databases import Data, DBInterface, DBEnums

from enum import Enum, auto

class DBTypes(Enum):
    SQLITE = auto()

def new_db_interface(*, engine, server="", user="", password="", encryption="", database=""):
    types = {"sqlite": DBTypes.SQLITE,
             "sqlite3": DBTypes.SQLITE}
    if not isinstance(engine, DBTypes) and engine in types:
        engine = types[engine]
    else:
        raise TypeError("dbtype must be a DBTypes instance or a correct string from configuration")
    return {DBTypes.SQLITE: SqliteInterface}[engine](database=database, server=server, user=user, password=password, encryption=encryption)
