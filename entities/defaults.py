#Default entities

from .fields import Field, Fields
from .entities import Entity
from ..databases.database import DBInterface, PRIMARY

def persistent(database): #TODO set datatypes more generally
    assert isinstance(database, DBInterface
    fields = Fields("__entities",
                    {"name": [str, PRIMARY]],
                     "table": str,
                     "description": str,
                     "parent": str,
                     "parent_field": str})
