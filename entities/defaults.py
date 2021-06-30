#!/usr/bin/env python

__author__ = "Iván Uría"

"""This module gives persistency default functions.
Functions:
    persistent: Creates Fields and Entity objects for __entities and __fields tables
    get_entity: Creates and returns an Entity object from table name
    get_entities: Creates and returns all Entity object from database
    install_persistency: Creates tables __entities and __fields in database
"""

import asyncio
from .fields import Field, Fields
from .entities import Entity
from databases.databases import DBInterface, DBEnums
from datetime import datetime
from typing import NoReturn

def persistent(database:DBInterface, *, loop:asyncio.BaseEventLoop=None) -> tuple:
    """Returns a tuple with two Entitiy: the entity of entities and the entity of fields
    Arguments:
        database: DBInterface to play with
    Key Arguments:
        loop: asyncronous loop to give a real-time actualization. Optional.
    """
    assert isinstance(database, DBInterface)
    ent_fields = Fields(database, "__entities",
                    {"name": str,
                     "table_name": [str, DBEnums.PRIMARY],
                     "description": str,
                     "parent": str,
                     "parent_field": str})
    entity = Entity(database, "__entities", "Entities",
                    ent_fields,
                    "Entities description",
                    loop=loop)

    field_fields = Fields(database, "__fields",
                          {"name": str,
                           "definition": str,
                           "description": str,
                           "table_name": str})
    fields_entity = Entity(database, "__fields", "Fields",
                          field_fields,
                          "Fields Description",
                          entity,
                          "table",
                          loop=loop)
    return entity, fields_entity

def get_entity(database:DBInterface, table:str, ent:dict=None, *, loop:asyncio.BaseEventLoop=None) -> Entity:
    """Returns an Entity instance of the indicated table.
    Arguments:
        database: DBInterface to play with
        table: name of the table
        ent: dictionary of data to instantiate new Entity if needed. Optional
            {"table_name": str,
             "name": str,
             "description": str,
             "parent": str,
             "parent_field": str}
    Key Arguments:
        loop: asyncronous loop to give a real-time actualization. Optional.
    """
    if database in Entity.persistent and table in Entity.persistent[database]:
        return Entity.persistent[database][table]
    entity, fields_entity = persistent(database)
    flst = fields_entity.get({"table_name": table})
    fields = Fields(database, table, [Field(database, table, item["name"], eval(item["definition"]), description=item["description"]) for item in flst])
    fields.set_installed()
    if ent is None:
        ent = entity.get({"table_name": table})[0]
    if ent:#TODO: raise especial exception if not exists
        return Entity(database, ent["table_name"], ent["name"], fields, ent["description"], parent=ent["parent"], parent_field=ent["parent_field"], loop=loop)
    else:
        return None

def get_entities(database:DBInterface, *, loop:asyncio.BaseEventLoop=None) -> list:
    """Returns a list of entities from database got from get_entity in a for loop
    Arguments:
        database: DBInterface to play with
    Key Arguments:
        loop: asyncronous loop to give a real-time actualization. Optional.
    """
    entity, fields_entity = persistent(database, loop=loop)
    entities_list = entity.get({})
    final = {}
    for ent in entities_list:
        final[ent["name"]] = get_entity(database, ent["table_name"], ent=ent)
    return final

def install_persistency(database:DBInterface) -> NoReturn:
    """Creates the tables __fields and __entities
    Arguments:
        database: DBInterface to play with
    """
    entity, fields_entity = persistent(database)
    entity.install()
    fields_entity.install()
