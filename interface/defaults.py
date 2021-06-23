from databases import DBEnums
from datetime import datetime
def hasher(pwd, salt=os.urandom(32)):
    if isinstance(salt, str):
        salt = salt.encode("utf-8")
    return hashlib.pbkdf2_hmac("sha256", pwd.encode("utf-8"), salt, 100000)
    # https://nitratine.net/blog/post/how-to-hash-passwords-in-python/

DEFINITIONS = {"__users": {"id": [str, DBEnums.PRIMARY],
                           "name": str,
                           "pwdhash": str,
                           "token": str,
                           "created_at": datetime,
                           "expires_at": datetime,
                           "roles": str},
               "__roles": {"id": [str, DBEnums.PRIMARY],
                           "name": str,
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
                  "name": "Administrator",
                  "description": "System Administrator",
                  "parent": None},
                 {"id": "manager",
                  "name": "Manager",
                  "description": "Production Manager",
                  "parent": "admin"},
                 {"id": "user",
                  "name": "User",
                  "desctiption": "System User",
                  "parent": "manager"},
                 {"id": "itmanager",
                  "name": "IT Manager",
                  "description": "IT Team Manager",
                  "parent": "admin"},
                 {"id": "ituser",
                  "name": "IT User",
                  "description": "IT User",
                  "parent": "itmanager"}]

DEFAULT_PERMISSIONS = [{"entity"}]

DEFAULT_PWD = "simp123"

DEFAULT_USERS = [{"id": "opm001",
                  "name": "Manager 1",
                  "pwdhash": hasher(DEFAULT_PWD, "opm001"),
                  "token": "",
                  "roles": "manager user"},
                 {"id": "op001",
                  "name": "Operator 1",
                  "pwdhash": hasher(DEFAULT_PWD, "op001"),
                  "token": "",
                  "roles": "user"},
                 {"id": "op002",
                  "name": "Operator 2",
                  "pwdhash": hasher(DEFAULT_PWD, "op002"),
                  "token": "",
                  "roles": "user"},
                 {"id": "itm001",
                  "name": "IT Manager 1",
                  "pwdhash": hasher(DEFAULT_PWD, "itm001"),
                  "token": "",
                  "roles": "itmanager ituser"},
                 {"id": "it001",
                  "name": "IT User 1",
                  "pwdhash": hasher(DEFAULT_PWD, "it001"),
                  "token": "",
                  "roles": "ituser"},
                 {"id": "it002",
                  "name": "IT User 2",
                  "pwdhash": hasher(DEFAULT_PWD, "it002"),
                  "token": "",
                  "roles": "ituser"}]