import argparse
import os
import sys
from configparser import ConfigParser

DEFAULT_VARS = {"--config": "config"}

def main(config):
    print("No Interface indicated")

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Instantiates new SimpCRM')

    #New path for config.ini
    parser.add_argument("--config", help="Path to the config file, defaults relative to this file")
    #New Database
    parser.add_argument("--engine", help="Engine for instantiate database")
    parser.add_argument("--server", help="Server for database")
    parser.add_argument("--user", help="User to access database")
    parser.add_argument("--password", help="Password to access database")
    parser.add_argument("--database", help="Database name")

    args = DEFAULT_VARS.copy()
    args.update(vars(parser.parse_args(sys.argv[1:])))
    
    #Cleaning
    args["--config"] = os.path.normpath(args["--config"])

    config = ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), args["--config"], "config.ini"))
    print(os.path.join(os.path.dirname(os.path.realpath(__file__)), args["--config"], "config.ini"))
    print(config.sections())

    #Updating Main DB if needed
    if "Main DB" in config.sections():
        for key in ["engine", "server", "user", "password", "database"]: 
            if "--"+key in args:
                config["Main DB"][key] = args["--"+key]

    if "Interface" in config.sections():
        if config["Interface"]["default"] == "tkinter":
            from interface.tkinterface import main
        main(config)