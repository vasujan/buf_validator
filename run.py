import streamlit.web.cli as stcli
import os
import sys
import configparser

config = configparser.ConfigParser()
config.read("src/config.ini")
ST_APP = config["APP"]["main_page"]

def resolve_path(*path):
    resolved_path = os.path.abspath(os.path.join(os.getcwd(), *path))
    return resolved_path

if __name__ == "__main__":
    sys.argv = [
        "streamlit", "run",
        resolve_path("src", ST_APP),
        "--global.developmentMode=False"
    ]

    sys.exit(stcli.main())