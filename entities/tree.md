## Entities Tree

__init__.py
entities.py
  |_ Entity
    @properties
    |_ entity_id
    |_ parent (0 if none)
    |_ name
    |_ description
    |_ fields (Fields())
    @methods
    |_ __init__ (id, parent, name, description, fields)
    |_ get (filter)
    |_ update (filter, Fields())
    |_ remove (filter)
    |_ insert (Fields())
  |_ Fields (List of Field)
    |_ __init__(dictionary, tuple, list)
    |_ add_field ()
    |_ remove_field ()
  |_ Field
    @property
    |_ field_id
    |_ field_name
    |_ field_type
    |_ field_max
    |_ field_description
  |_ load_entities -> meta loading of configuration file of entities

default.py (default entities)
