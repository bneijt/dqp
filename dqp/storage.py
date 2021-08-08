import os
from typing import Dict

import msgpack

VARS_FILENAME = "vars.msgpack"


class Folder:
    """Class to help manage a folder structure with state,
    including a default vars object for state.
    """

    def __init__(self, path: str):
        """
        Open the folder at path, creating the folder if it does not exist
        """
        self.path = path
        self.open()

    def open(self) -> None:
        self.create_path("")
        vars_filename = os.path.join(self.path, VARS_FILENAME)
        self.vars = {}  # type: Dict[str, str]
        if os.path.exists(vars_filename):
            with open(vars_filename, "rb") as env_file:
                self.vars = msgpack.load(env_file)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback) -> None:
        self.close()

    def child(self, sub_path: str) -> str:
        return os.path.join(self.path, sub_path)

    def create_path(self, sub_path: str) -> str:
        full_path = self.child(sub_path)
        os.makedirs(full_path, exist_ok=True)
        return full_path

    def close(self) -> None:
        """
        Close the folder, this will flush the vars to disk
        """
        vars_path = self.child(VARS_FILENAME)
        if len(self.vars) or os.path.exists(vars_path):
            with open(vars_path, "wb") as vars_file:
                msgpack.dump(self.vars, vars_file)
