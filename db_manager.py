
import sqlite3
from sqlite3.dbapi2 import Connection
from numpy import array

from pathlib import Path
from os      import remove  as remove_file
from os.path import exists  as file_exists

from typing import Optional

class DbManager:
    """
    This class is used to manage the connection to the database.
    Allowing to quickly connect and disconnect from the database.
    """
    def __init__(self, sqlite_file_name:Path):
        """Instantiate a new connection the sqlite database"""
        assert sqlite_file_name.suffix == ".db", "Not a valid db file name ({sqlite_file_name})" 
        self.sqlite_file_name = sqlite_file_name
        self.sqlite_conn: Connection = self._init_sqlite()

    def _init_sqlite(self, first_sql:Optional[str]=None) -> Connection:
        """Return a connection to the sqlite database but checking if a connection already exists"""
        sqlite_conn = sqlite3.connect(self.sqlite_file_name)
        if first_sql is not None:
            self.write_on_db(first_sql)
        return sqlite_conn

    def _detach_sqlite(self)-> bool:
        """Close the connection to the sqlite database and remove the db file"""
        self.sqlite_conn.close()
        if file_exists(self.sqlite_file_name):
            remove_file(self.sqlite_file_name)
        return file_exists(self.sqlite_file_name)

    def write_on_db(self, sql:str):
        """
        Used to write some data on the database (insert, update, delete).
        It also check for the connection before writing.
        """
        if self.sqlite_conn is not None:
            self.sqlite_conn.execute(sql)
        else:
            raise Exception("Writing sql without connection")

    def get_num_elements(self, table_name:str)-> int:
        """Return the number of elements in the table"""
        return self.sqlite_conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]


if __name__ == "__main__":
    db = DbManager(Path("test.db"))

