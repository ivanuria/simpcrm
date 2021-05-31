# Main program

from entities import Item, Entity
from entities.default import install_persistency, get_entity, get_entities
from database import DBInterface, new_db_interface
from configparser import ConfigParser

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

    def install(self):
        install_persistency(self.database)

    def load(self):
        self.entities = get_entities(self.database)
