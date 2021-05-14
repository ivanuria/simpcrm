## Databases Tree

__init__.py
  |_ reading of config file, getting sqlite or mysql
dbinterface.py
  |_ database interface
    @properties
    |_ database
    |_ table
    |_ filter
    @methods
    |_ connect
    |_ disconnect
    |_ set_database
    |_ set_table
    |_ set_filter
    |_ insert
    |_ update
    |_ select
    |_ remove
  |_ Data
    -> inheriting from list and giving results in a dictionary
sqlite.py
  |_ inherits from database interface, sets methods to use sqlite
mysql.py
  |_ inherits from database interface, sets methods to use mysql
