# Main program

import asyncio
import os
import threading
import time
from entities import Item, Entity
from entities.defaults import install_persistency, get_entity, get_entities
from databases import DBInterface, new_db_interface, DBEnums
from collections import defaultdict
from configparser import ConfigParser
from datetime import datetime, timedelta
from functools import wraps
from .defaults import *

VERSION = "0.1"

EXPIRE_TOKEN = 3600

#Decorator for user permissions:
def only_permitted(table=None, operation="r"):
    def only_permitted_decorator(func):
        @wraps(func)
        def only_permitted_wrapper(self, *args, table=table, operation=operation, **kwargs):
            if self.installed is False:
                raise RuntimeError("Not installed yet")
            if all([i in kwargs for i in ["user", "token"]]):
                user, token = kwargs["user"], kwargs["token"]
                if isinstance(table, int) and len(args) > table:
                    table = args[table]
                elif table is None and not "table" in kwargs:
                    raise AttributeError("Table not found")
                elif table is None:
                    table = kwargs["table"]
                if self.logged(user, token) is False:
                    raise RuntimeError("Unauthorised: may login again")
                else:
                    user = self.entities["__users"][user]
                    if not user:
                        raise RuntimeError("User not found")
                    else:
                        user = user
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
    return only_permitted_decorator

class Main:
    def __init__(self, *, configdbfile=os.path.join("config", "config.ini"), config=None):
        """
        Main implementation to separate it from GUI
        """
        self._configdbfile = configdbfile
        if config is None:
            self._config = self.read_configuration(configdbfile)
        else:
            self._config = config
        self._database = new_db_interface(**self._config["Main DB"])
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever)
        self._thread.start()

    def __del__(self):
        try:
            self.close()
        except (AttributeError, RuntimeError): #In case it's already closed or not opened
            pass

    def __str__(self):
        return " - ".join((str(self._config), str(self.entities)))

    #static Methods
    @classmethod
    def read_configuration(cls, configdbfile):
        default_config = {"Main DB": {"engine": "sqlite",
                                     "server": "",
                                     "user": "",
                                     "password": "",
                                     "encryption": "",
                                     "database": "data.db"}
                          }
        config = ConfigParser()
        config.read(configdbfile)
        if "Main DB" in config.sections():
            default_config["Main DB"].update(dict(config["Main DB"]))
        default_config["Main DB"]["database"] = default_config["Main DB"]["database"].split("\\")
        final = []
        default_config["Main DB"]["database"] = [final.extend(i.split("/")) for i in default_config["Main DB"]["database"]]
        default_config["Main DB"]["database"] = os.path.join(*final)
        return default_config

    @classmethod
    def save_configuration(cls, path_to_save, data):
        config = ConfigParser()
        if "Main DB" in data:
            config["Main DB"] = data["Main DB"]
        with open(path_to_save, "w") as cfgfile:
            config.write(cfgfile)

    @property
    def database(self):
        return self._database

    @property
    def entities(self):
        return Entity.persistent[self.database]

    @property
    def installed(self):
        try:
            get_entity(self.database, "__simpcrm_main")
        except Exception as e:
            return False
        else:
            return True

    def close(self):
        self.database.disconnect()
        self._loop.call_soon_threadsafe(self._loop.stop)
        while True:
            if self._loop.is_running() is False:
                self._loop.close()
                break
            time.sleep(1)
        self._thread.join()

    def install(self, user, name, password):
        if self.installed is False:
            install_persistency(self.database)
            for table in DEFINITIONS:
                Entity(self.database, table, table, DEFINITIONS[table], "", loop=self._loop)
                self.entities[table.split(":")[-1]].install()
            salt, password_hash = hasher(password)
            self.entities["__users"].insert({"id": user,
                                             "name": name,
                                             "salt": salt,
                                             "pwdhash": password_hash,
                                             "roles": "admin"})
            self.entities["__users"].insert(DEFAULT_USERS)
            self.entities["__roles"].insert(DEFAULT_ROLES)
            self.entities["__permissions"].insert(DEFAULT_PERMISSIONS)
            self.entities["__simpcrm_main"].insert({"installed": datetime.now(),
                                                    "version": VERSION,
                                                    "name": "",
                                                    "description": ""})

    def load(self):
        if self.installed is True:
            get_entities(self.database, loop=self._loop)

    def logged(self, user, token):
        try:
            user = self.entities["__users"][user]
        except IndexError:
            raise RuntimeError("User not found")
        if user["token"] != token or user["expires_at"] < datetime.now():
            return False
        else:
            return True

    def login(self, user, pwd):
        try:
            user = self.entities["__users"][user]
        except IndexError:
            raise RuntimeError("User not found")
        _, pwdhash = hasher(pwd, user["salt"])
        if user["pwdhash"] == pwdhash:
            salt, user["token"] = hasher("".join(map(str,(user["id"], pwdhash))))
            user["expires_at"] = datetime.now() + timedelta(seconds=EXPIRE_TOKEN)
        return user["token"]

    def get_role_children(self, role_id):
        roles = self.entities["__roles"].get()
        if isinstance(role_id, str):
            role_id = role_id.split(" ")
        #roles = list(role_id)
        final_roles = []
        #roles_id = [role["id"] for role in roles]
        if "admin" in role_id:
            final_roles.append("admin")
        while True: #supposedly not a lot of roles and only used by few users
            added = False
            for role in roles.copy():
                if role["parent"] in role_id+final_roles:
                    final_roles.append(role["id"])
                    del(roles[roles.index(role)])
                    added = True
            if added is False:
                break
        return final_roles

    # USERS
    def check_permitted_roles(self, user, roles):
        if isinstance(roles, str):
            roles = roles.split(" ")
        l_roles = list(roles)
        accepted_roles = self.get_role_children(self.entities["__users"][user]["roles"])
        final_roles = []
        for role in l_roles:
            if role in accepted_roles:
                final_roles.append(role)
        return final_roles

    @only_permitted(table="__users", operation="w")
    def new_user(self, new_user, name, password_hash, roles, *, user, token):
        if not self.entities["__users"][new_user]:
            final_roles = self.check_permitted_roles(user, roles)
            self.entities["__users"][new_user] = {"id": new_user,
                                                  "name": name,
                                                  "pwdhash": password_hash,
                                                  "roles": " ".join(final_roles)}
        else:
            raise RuntimeError("User already exists")

    @only_permitted(table="__users", operation="w")
    def modify_user(self, new_user, name, password_hash, roles, *, user, token):
        if self.entities["__users"][new_user]:
            final_roles = self.check_permitted_roles(user, roles)
            self.entities["__users"][new_user] = {"id": new_user,
                                                  "name": name,
                                                  "pwdhash": password_hash,
                                                  "roles": " ".join(final_roles)}
        else:
            self.new_user(new_user, name, password_hash, roles, user=user, token=token)
            # Modify a non existing user should create it, why not?

    @only_permitted(table="__users", operation="w")
    def delete_user(self, user_id, *, user, token):
        if self.entities["__users"][user_id]:
            roles = self.entities["__users"][user]["roles"].split(" ")
            roles = self.check_permitted_roles(user, roles)
            user_roles = self.entities["__users"][user_id]["roles"].split(" ")
            if any([role in user_roles for role in roles]):
                self.entities["__users"].delete({self.entities["__users"].primary_key: user_id})
            else:
                raise RuntimeError("Operation not permmited")
        else:
            raise RuntimeError("User not found")

    # ROLES and permissions
    @only_permitted(table="__permissions", operation="r")
    def get_user_permissions(self, user_id, *, user, token):
        permissions = defaultdict(lambda: defaultdict(False)) # Entity[operation]
        for perm in self.entities["__permissions"].get({"__roles_id": ["IN", self.entities["__users"][user_id]["roles"]]}):
            if perm["permitted"] is True:
                permissions[perm["entity"]][perm["operation"]] = perm["permitted"]
        return permissions

    def get_self_permissions(self, *, user, token):
        permissions = defaultdict(lambda: defaultdict(lambda: False))
        if self.logged(user, token) is False:
            raise RuntimeError("Unauthorised: may login again")
        else:
            user = self.entities["__users"][user]
            roles = user["roles"].split(" ")
            authorising = self.entities["__permissions"].get({"__roles_id": ["IN", roles]})
        for perm in authorising:
            if perm["permitted"] is True:
                permissions[perm["entity"]][perm["operation"]] = perm["permitted"]
        return permissions

    def get_permited_permissions_changes(self, user, token, permissions):
        permitted_changes = self.get_self_permissions(user=user, token=token)
        final = []
        if user == "admin":
            return permissions
        for i in permissions:
            assert all([item in i for item in ["entity", "operation", "permitted"]])
            ent = i["entity"]
            op = i["operation"]
            if ent in permitted_changes and permitted_changes[ent][op] is True:
                final.append(i)
        return final

    @only_permitted(table="__permissions", operation="w")
    def new_role(self, role_id, description, parent, permissions, *, user, token):
        permitted_changes = self.get_permited_permissions_changes(user, token, permissions)
        if self.entities["__roles"][role_id]:
            raise RuntimeError("Role already existing")
        else:
            self.entities["__roles"][role_id] = {"description": description,
                                                 "parent": parent}
            for perm in permitted_changes:
                self.entities["__permissions"].insert({"entity": perm["entity"],
                                                       "operation": perm["operation"],
                                                       "permitted": perm["permitted"],
                                                       "__roles_id": role_id})

    @only_permitted(table="__permissions", operation="w")
    def modify_role(self, role_id, description, parent, permissions, *, user, token):
        if role_id == "admin":
            for item in permissions.copy():
                if "entity" in item and item["entity"] in list(DEFINITIONS.keys()):
                    del permissions[permissions.index(item)]
        permitted_changes = self.get_permited_permissions_changes(user, token, permissions)
        this_role = self.entities["__roles"][role_id]
        if not this_role:
            self.new_role(role_id, description, parent, permissions, user=user, token=token)
            #By now any attempt of changing a non existing role will create
        else:
            if description is None:
                description = this_role["description"]
            if parent is None:
                parent = this_role["parent"]
            self.entities["__roles"][role_id] = {"description": description,
                                                 "parent": parent}
            for perm in permitted_changes:
                this = self.entities["__permissions"].get({"entity": perm["entity"],
                                                           "operation": perm["operation"],
                                                           "__roles_id": role_id})
                if not this:
                    self.entities["__permissions"].insert({"entity": perm["entity"],
                                                           "operation": perm["operation"],
                                                           "permitted": perm["permitted"],
                                                           "__roles_id": role_id})
                else:
                    self.entities["__permissions"].replace({"id": this[0]["id"]},
                                                           {"permitted": perm["permitted"]})

    @only_permitted(table="__permissions", operation="w")
    def delete_role(self, role_id):
        if role_id == "admin":
            raise RuntimeError("Operation not permitted") # TODO: Make another exception
        permitted_changes = self.check_permitted_roles(user, [role_id])
        if role_id not in permitted_changes:
            raise RuntimeError("Operation not permitted")
        else:
            self.entities["__permissions"].delete({"id": role_id})

    # Entities creation
    @only_permitted(table="__entities", operation="w")
    def new_entity(self, entity_id, name, fields, description, parent="", parent_field="", *, user, token):
        if entity_id in self.entities:
            raise RuntimeError("Entity already defined")
        elif entity_id.startswith("__"):
            raise RuntimeError("Entity Id not supported")
        else:
            if isinstance(fields, (list, tuple)):
                assert all([key in fields for key in ["name", "definition"]])
            elif isinstance(fields, dict):
                assert all([isinstance(fields[key], type) for key in fields])
            self.entities[entity_id] = Entity(self._database, entity_id, name,
                                              fields, description, parent,
                                              parent_field, loop=self._loop)
            self.entities[entity_id].install()

    @only_permitted(table="__entities", operation="w")
    def modify_entity(self, entity_id, name, fields, description, parent, parent_field, *, user, token):
        if entity_id.startswith("__"):
            raise RuntimeError("Entity Id not supported")
        elif entity_id in self.entities:
            to_change = []
            to_add = []
            for field in fields:
                assert all([key in field for key in ["name", "definition", "description", "table_name"]])
                if any([key in field for key in ["new_definition", "new_name", "new_description"]]):
                    to_change.append(field)
                if field["name"] not in self.entities[entity_id].fields:
                    to_add.append(field)
            if to_change:
                self.entities[entity_id].change_fields(to_change) # Safaty first even if it makes this slower
            if to_add:
                self.entities[entity_id].add_fields(to_add)
            self.entities[entity_id] = Entity(self._database, entity_id, name, fields, description, loop=self._loop)
        else:
            self.new_entity(entity_id, name, fields, description, parent, parent_field, User=user, token=token)

    @only_permitted(table="__entities", operation="w")
    def delete_entity(self, entity_id):
        if entity_id.startswith("__"):
            raise RuntimeError("Entity Id not supported")
        elif entity_id in self.entities:
            self.entities[entity_id].uninstall()

    #In Entities operations
    @only_permitted(table=0, operation="r")
    def get_data(self, entity_id, filter, *, user, token):
        if entity_id in self.entities:
            return self.entities[entity_id].get(filter)

    @only_permitted(table=0, operation="w")
    def add_data(self, entity_id, data, *, user, token):
        if entity_id in self.entities:
            self.entities[entity_id].insert(data)

    @only_permitted(table=0, operation="w")
    def replace_data(self, entity_id, filter, data, *, user, token):
        if entity_id in self.entities:
            self.entities[entity_id].replace(filter, data)

    @only_permitted(table=0, operation="w")
    def delete_data(self, entity_id, filter, *, user, token):
        if entity_id in self.entities:
            self.entities[entity_id].delete(filter)
