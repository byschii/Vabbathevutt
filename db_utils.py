import sqlite3
from sqlite3.dbapi2 import Connection
from numpy import array

from pathlib import Path
from os      import remove  as remove_file
from os.path import exists  as file_exists

from typing import Any, List, Literal, Optional, Union


class DbManager:
    def __init__(self, sqlite_file_name:Path):
        assert sqlite_file_name.suffix == ".db", "Not a valid db file name ({sqlite_file_name})" 
        self.sqlite_file_name = sqlite_file_name
        self.sqlite_conn: Connection = self._init_sqlite()

    def _init_sqlite(self, first_sql:Optional[str]=None) -> Connection:
        sqlite_conn = sqlite3.connect(self.sqlite_file_name)
        if first_sql is not None:
            self.write_on_db(first_sql)
        return sqlite_conn

    def _detach_sqlite(self)-> bool:
        self.sqlite_conn.close()
        if file_exists(self.sqlite_file_name):
            remove_file(self.sqlite_file_name)
        return file_exists(self.sqlite_file_name)

    def write_on_db(self, sql:str):
        if self.sqlite_conn is not None:
            self.sqlite_conn.execute(sql)
        else:
            raise Exception("Writing sql without connection")

    def get_num_elements(self, table_name:str)-> int:
        return self.sqlite_conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]

class TableHandler:
    def __init__(self, db_connection:DbManager, table_name:str, table_size:int):
        self.db_connection = db_connection
        self.table_name = table_name
        self.table_size = table_size
        self._init_table()

    def _init_table(self):
        """
        if table already exists
        it s not rebuilt
        """
        if not self._table_exists() or not self._get_num_rows_in_table() > 0:
            self.db_connection.write_on_db(self._get_table_creation_query())

    def _drop(self):
        self.db_connection.sqlite_conn.execute(f"DROP TABLE IF EXISTS {self.table_name};")

    def _get_table_creation_query(self)-> str:
        return f"""CREATE TABLE IF NOT EXISTS {self.table_name} (
            id_{self.table_name} INTEGER PRIMARY KEY,
                {str(
                    ", ".join([ f"{self.table_name}_val_{str(i)} REAL NOT NULL" for i in range(self.table_size) ])
            )}
        );"""

    def _table_exists(self):
        sql = f"SELECT * FROM sqlite_master where name='{self.table_name}'"
        query_result = self.db_connection.sqlite_conn.execute(sql).fetchall()
        return len(query_result) == 1

    def _get_num_rows_in_table(self) -> int:
        """
        costa circa 0.01sec
        """
        num_arrays = self.db_connection.sqlite_conn.execute(f"""SELECT count(*) FROM {self.table_name}""")
        return num_arrays.fetchone()[0]

    def dump_table(self, as_array:Optional[bool]=True)-> Any:
        sql = f"SELECT * FROM {self.table_name}"
        result = self.db_connection.sqlite_conn.execute(sql).fetchall() 
        return array(result) if as_array else result 

    def create_row(self, pk:int, row_values:List[Any]):
        assert len(row_values) == self.table_size, f"Wrong size. Array length={len(row_values)}, required length={self.table_size}"
        _INSERT_INTO_TABLE = f"""INSERT INTO {self.table_name} VALUES (
            {pk}, 
            {str(row_values)[1 : -1]}
        );"""
        self.db_connection.write_on_db(_INSERT_INTO_TABLE)

    def update_row(self, pk:int, row_values:List[Any]):
        assert len(row_values) == self.table_size, f"Wrong size. Array length={len(row_values)}, required length={self.table_size}"
        _INSERT_INTO_TABLE = f"""UPDATE {self.table_name} SET 
            
            {str(
                    ", ".join([ f"{self.table_name}_val_{str(i)} = {row_values[i]} " for i in range(self.table_size) ])
            )}
        
        WHERE id_{self.table_name} = {pk};"""
        self.db_connection.write_on_db(_INSERT_INTO_TABLE)

    def delete_row(self, pk:int)-> bool:
        delete_cursor = self.db_connection.sqlite_conn.execute(f"DELETE FROM {self.table_name} WHERE id_{self.table_name} = {pk}")
        return delete_cursor.rowcount == 1

    def get_row(self, pk:int)-> List[Any]:
        select_cursor = self.db_connection.sqlite_conn.execute(f"SELECT * FROM {self.table_name} WHERE id_{self.table_name} = {pk}")
        return select_cursor.fetchone()[1:]


