# databases.py
# Databases init package

VERSION = 0.1

from .sqlite import SqliteInterface
from .databases import Data, DBInterface

from enum import Enum, auto

class DBTypes(Enum):
    SQLITE = auto()

def new_db_interface(dbtype, server="", user="", password="", encription="", database=""):
    return {DBTypes.SQLITE: SqliteInterface(server="", user="", password="", encription="", database="")}
