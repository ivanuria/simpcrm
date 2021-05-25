#Default entities

from .fields import Field, Fields
from .entities import Entity
from ..databases.database import DBInterface, PRIMARY

def persistent(database):
    assert isinstance(database, DBInterface)
    ent_fields = Fields("__entities",
                    {"name": [str, PRIMARY]],
                     "table": str,
                     "description": str,
                     "parent": str,
                     "parent_field": str})
    entity = Entity("Entities",
                    "__entities",
                    ent_fields,
                    "Entities description",
                    database)

    field_fields = Fields("__fields",
                          {"name": str,
                           "definition": str,
                           "description": str,
                           "table": str})
    field_entity = Entity("Fields",
                          "__fields",
                          field_fields,
                          "Fields Description",
                          database,
                          entity,
                          "table")

    return entity, fields_entity

def get_entity(database, table, ent=None):
    entity, fields_entity = persistent(database)
    flst = fields_entity.get({"table": table})
    fields = Fields([Field(item["name"], item["definition"], description=item["description"]) for item in flst])
    if ent is None:
        ent = entity.get({"table": table})[0]
    return Entity(ent["name"], ent["table"], fields, ent["description"], database, parent=ent["parent"], parent_field=ent["parent_field"])

def get_entities(database):
    entity, fields_entity = persistent(database)
    entities_list = entity.get({})
    final = {}
    for ent in entities_list:
        final[ent["name"]] = get_entity(database, ent["table"], ent=ent)
    return final
def install_persistency(database):
    entity, fields_entity = persistent(database)
    entity.install()
    fields_entity.install()
