# Main program

from entities import Item, Entity
from entities.default import install_persistency, get_entity, get_entities
from database import DBInterface, new_db_interface, DBEnums
from configparser import ConfigParser
from datetime import datetime

VERSION = "0.1"

DEFINITIONS = {"__users": {"id": [str, DBENums.PRIMARY],
                           "name": str,
                           "pwdhash": str,
                           "token": str,
                           "created_at": datetime,
                           "expires_at": datetime,
                           "roles": str},
               "__roles": {"id": [str, DBEnums.PRIMARY],
                           "description": str},
               "__roles:__permissions": {"id": [int, DBEnums.PRIMARY],
                                         "entity": str,
                                         "operation": str,
                                         "permitted": bool,
                                         "__roles_id": int},
               "__simpcrm_main": {"id": [int, DBEnums.PRIMARY],
                                  "installed": datetime,
                                  "version": str,
                                  "name": str,
                                  "description": str}
               }

DEFAULT_ROLES = [{"id": "admin",
                  "name": "Administrador",
                  "description": "Administrador del sistema"},
                 {"id": "user",
                  "name": "Usuario",
                  "desctiption": "Usuario del sistema"}]

class Main:
    def __init__(self, configdbfile="config\databases.ini"):
        """
        Main implementation to separate it from GUI
        """
        self._configdbfile = configdbfile
        self._config = self.read_configuration(configfile)
        self._database = new_db_interface(**self._config["Main DB"])
        if self.installed is False:
            self.install()
        else:
            self.load()

    #static Methods
    @classmethod
    def read_configuration(cls, configfile):
        default_config = {"Main DB": "type": "sqlite",
                                     "server": "data.db",
                                     "user": "",
                                     "password": "",
                                     "encription": "",
                                     "database": ""}
        config = ConfigParser()
        config.read(configdbfile)
        if "Main DB" in config.sections():
            default_config.update(dict(config))
        return default_config

    @classmethod
    def save_configuration(cls, path_to_save, data):
        config = ConfigParser()
        if "Main DB" in data:
            config["Main DB"] = data["Main DB"]
        with open(path_to_save, "w") as cfgfile:
            config.write(configfile)

    @property
    def installed(self):
        try:
            get_entity("__simpcrm_main")
        except:
            return False
        else:
            return True

    def install(self, user, name, password_hash):
        install_persistency(self.database)
        for table in DEFINITIONS:
            self.entities[table] = Entity(self.database, table, table,DEFINITIONS[table], "")
            self.entities[table].install()
        self.entities["__users"].insert({"id": user,
                                         "name": name,
                                         "pwdhash": password_hash,
                                         "roles": "admin"})
        self.entities["__roles"].insert(DEFAULT_ROLES)
        permissions = []
        for role, perm in (("admin", True), ("user", False)):
            for ent in ["__users", "__permissions", "__simpcrm_main"]:
                for op in ["read", "write", "delete"]:
                    permissions.append({"__roles_id": role, "entity": ent, "operation": op, "permitted": perm})
        self.entities["__simpcrm_main"].insert({"installed": datetime.now(),
                                                "version": VERSION,
                                                "name": "",
                                                "description": ""})

    def load(self):
        self.entities = get_entities(self.database)
