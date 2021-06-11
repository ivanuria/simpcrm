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
                if self.logged(user, token) is False:
                    raise RuntimeError("Unauthorised: may login again")
                else:
                    user = self.entities["__users"][user]
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

    def logged(self, user, token):
        user = self.entities["__users"][user]
        if user["token"] != token or user["expires_at"] < datetime.datetime.now():
            return False
        else:
            return True

    def get_role_children(self, role_id):
        roles = self.entity["__roles"].get()
        if isinstance(role_id, str):
            role_id = role_id.split(" ")
        final_roles = list(role_id)
        while True: #supposedly not a lot of roles and only used by few users
            added = False
            for role in roles:
                if role["parent"] in final_roles:
                    final_roles.append(role["id"])
                    added = True
            if added is False:
                break
        return final_roles

    # USERS
    @only_permitted(table="__users", operation="w")
    def new_user(self, new_user, name, password_hash, roles, *, user, token):
        if not self.entities["__users"][new_user]:
            n_roles = roles.split(" ")
            accepted_roles = self.get_role_children(self.entity["__users"][user]["roles"])
            final_roles = []
            for role in list(n_roles):
                if role in accepted_roles:
                    final_roles.append(role)
            self.entities["__users"][new_user] = {"id": new_user,
                                                  "name": name,
                                                  "pwdhash": password_hash,
                                                  "roles": " ".join(final_roles)}
        else:
            raise RuntimeError("User already exists")

    @only_permitted(table="__users", operation="w")
    def modify_user(self, new_user, name, password_hash, roles, *, user, token):
        pass

    @only_permitted(table="__users", operation="w")
    def delete_user(self, user_id, **kwargs):
        self.entities["__users"].delete({self.entities["__users"].primary_key: user_id})


    # ROLES and permissions
    @only_permitted(table="__permissions", operation="r")
    def get_user_permissions(self, user_id, *, user, token):
        permissions = defaultdict(lambda: defaultdict(False)) # Entity[operation]
        for perm in self.entities["__permissions"].get({"__roles_id": ["IN", self.entities["__users"][user_id]["roles"]]}):
            if perm["permitted"] is True:
                permission[perm["entity"]][[perm["operation"]] = perm["permitted"]

    def get_self_permissions(self, *, user, token):
        permissions = defaultdict(lambda: defaultdict(False))
        if self.logged(user, token) is False:
            raise RuntimeError("Unauthorised: may login again")
        else:
            user = self.entities["__users"][user]
            roles = user["roles"].split(" ")
            authorising = self.entities["__permissions"].get({"entity": table,
                                                              "operation": operation,
                                                              "__roles_id": ["IN", roles]})
        for perm in self.entities["__permissions"].get({"__roles_id": ["IN", self.entities["__users"][user_id]["roles"]]}):
            if perm["permitted"] is True:
                permission[perm["entity"]][[perm["operation"]] = perm["permitted"]

    @only_permitted(table="__permissions", operation="w")
    def new_role(self, role_id, description, parent, permissions, *, user, token):
        permitted_changes = self.get_self_permissions(user=user, token=token)
        for key in permissions:
            if key in permitted_changes and permitted_changes[key]["permitted"] is True:
                assert all[k in permissions[key] for k in ["entity", "operation", "permitted"]]
                permitted_changes[key] = permissions[key]
        if self.entities["__roles"][role_id]:
            raise RuntimeError("Role already existing")
        else:
            self.entities["__roles"][role_id] = {"description": description,
                                                 "parent": parent}
            for perm in permitted_changes:
                self.entities["__permissions"].insert({"entity": permitted_changes[perm]["entity"],
                                                       "operation": permitted_changes[perm]["operation"],
                                                       "permitted": permitted_changes[perm]["permitted"],,
                                                       "__roles_id": role_id})
