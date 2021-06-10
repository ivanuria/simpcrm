# Main program

from entities import Item, Entity
from entities.default import install_persistency, get_entity, get_entities
from database import DBInterface, new_db_interface, DBEnums
from configparser import ConfigParser
from datetime import datetime
from functools import wraps

VERSION = "0.1"

DEFINITIONS = {"__users": {"id": [str, DBENums.PRIMARY],
                           "name": str,
                           "pwdhash": str,
                           "token": str,
                           "created_at": datetime,
                           "expires_at": datetime,
                           "roles": str},
               "__roles": {"id": [str, DBEnums.PRIMARY],
                           "description": str,
                           "parent": str},
               "__roles:__permissions": {"id": [int, DBEnums.PRIMARY],
                                         "entity": str,
                                         "operation": str,
                                         "permitted": bool,
                                         "__roles_id": str},
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

EXPIRE_TOKEN = 3600

#Decorator for user permissions:
def only_permitted(table=None, operation="r"):
    def only_permitted_decorator(func):
        @wraps(func)
        def only_permitted_wrapper(self, *args, **kwargs):
            if self.installed is False:
                raise RuntimeError("Not installed yet")
            if all([i in kwargs for i in ["user", "token"]]):
                user, token = kwargs["user"], kwargs["token"]
                if table is None and not "table" in kwargs:
                    raise AttributeError("Table not found")
                else:
                    table = kwargs["table"]
                #map(del, [kwargs["user"], kwargs["token"]])
                user = self.entities["__users"][user]
                if user["token"] != token or user["expires_at"] < datetime.datetime.now():
                    raise RuntimeError("Unauthorised: may login again")
                else:
                    roles = user["roles"].split(" ")
                    authorising = self.entities["__permissions"].get({"entity": table,
                                                                      "operation": operation,
                                                                      "__roles_id": ["IN", roles]})

                if any([k["permitted"] for k in authorising]):
                    return func(self, *args, **kwargs)
                else:
                    raise RuntimeError("Unauthorised")
            else:
                raise AttributeError("You may be an identified user")
    return only_permitted_wrapper

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
                for op in ["r", "w", "d"]: #r=read, w=write, d=delete
                    permissions.append({"__roles_id": role, "entity": ent, "operation": op, "permitted": perm})
        self.entities["__simpcrm_main"].insert({"installed": datetime.now(),
                                                "version": VERSION,
                                                "name": "",
                                                "description": ""})

    def load(self):
        self.entities = get_entities(self.database)


    # USERS
    @only_permitted(table="__users", operation="w", **kwargs)
    def new_user(self, new_user, name, password_hash, roles):
        self.entities["__users"][new_user] = {"id": new_user,
                                              "name": name,
                                              "pwdhash": password_hash,
                                              "roles": roles}

    @only_permitted(table="__users", operation="w", **kwargs)
    def modify_user(self, new_user, name, password_hash, roles):
        # same as new_user just for readability
        self.new_user(new_user, name, password_hash, roles)

    @only_permitted(table="__users", operation="w, **kwargs")
    def delete_user(self, user_id):
        self.entities["__users"].delete({self.entities["__users"].primary_key: user_id})


    # ROLES and permissions
    @only_permitted(table="__permissions", operation="r", **kwargs)
    def get_user_permissions(self, user_id):
        permissions = defaultdict(lambda: defaultdict(False)) # Entity[operation]
        for perm in self.entities["__permissions"].get({"__roles_id": ["IN", self.entities["__users"][user_id]["roles"]]}):
            if perm["permitted"] is True:
                permission[perm["entity"]][[perm["operation"]] = perm["permitted"]

    @only_permitted(table="__permissions", operation="w", **kwargs)
    def new_role(self, role_id, description, parent, permissions):
        pass #TODO
