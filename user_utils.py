from db_utils import *
from vector_utils import *

from os      import remove  as remove_file
from typing import Any, List, MutableMapping, Mapping, Optional, Union
from pathlib import Path

class VectorUserManager:
    def __init__(self, username:str, default_dir:str="./"):
        self.username = username
        self.default_path = Path(default_dir + self.username + "/")
        self.default_path.mkdir()        

        self.db = DbManager( self.default_path.joinpath(Path(self.username + ".db")) )
        self.vs:MutableMapping[str, VectorSpace] = {}

    def add_space(self, space_name:str, dimensions:int, description:str="", max_unsynched_vectors:int=0):
        self.vs[space_name] = VectorSpace(
            self.db,
            str(self.default_path.joinpath(Path(space_name))),
            dimensions,
            description,
            max_unsynched_vectors
        )

    def delete_space(self, space_name:str):
        self.vs[space_name]._delete_vector_space()
        del self.vs[space_name]

    def _destruct(self):
        # first, we delete avery table in the db
        for _, space in self.vs.items():
            space._delete_vector_space()
        # then we close connection and delete the db file
        self.db.sqlite_conn.close()
        remove_file(self.db.sqlite_file_name)
        # finally, we delete the default path were everything is stored
        self.default_path.rmdir()

