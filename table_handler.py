from sqlite3.dbapi2 import Connection
from numpy import array, array2string

from pathlib import Path
from os      import remove  as remove_file
from os.path import exists  as file_exists

from typing import Any, List, Optional
from db_manager import DbManager


class TableHandler:
    """
    This class is used to handle most common operations on a table.
    Expecially crafted to handle vectors at low level. 
    Every table shouls store vector from the same domain.
    """
    def __init__(self, db_connection:DbManager, table_name:str, table_size:int):
        self.db_connection = db_connection
        self.table_name = table_name
        self.table_size = table_size
        self._init_table()

    def _init_table(self):
        """
        Crates a table with a primary key and a column for every dimension of the vector to store.
        The table primary key column is named as id_{table_name}, every other column is named as {table_name}_val_{i}
        """
        _table_creation_query = f"""CREATE TABLE IF NOT EXISTS {self.table_name} (
            id_{self.table_name} INTEGER PRIMARY KEY,
                {str(
                    ", ".join([ f"{self.table_name}_val_{str(i)} REAL NOT NULL" for i in range(self.table_size) ])
            )}
        );"""
        if not self._table_exists() or not self._get_num_rows_in_table() > 0:
            self.db_connection.write_on_db(_table_creation_query)

    def _drop(self):
        """Delete the table from the database."""
        self.db_connection.sqlite_conn.execute(f"DROP TABLE IF EXISTS {self.table_name};")

    def _table_exists(self):
        """
        Check if a table exists in the sqlite databese.
        This function is usefull if you wnat to know if the table has been dropped or not.
        """
        sql = f"SELECT * FROM sqlite_master where name='{self.table_name}'"
        query_result = self.db_connection.sqlite_conn.execute(sql).fetchall()
        return len(query_result) == 1

    def _get_num_rows_in_table(self) -> int:
        """
        A function to get the number of vectors in the table.
        It is typically executed in 0.01sec-
        """
        num_arrays = self.db_connection.sqlite_conn.execute(f"""SELECT count(*) FROM {self.table_name}""")
        return num_arrays.fetchone()[0]

    def dump_table(self, as_array:Optional[bool]=True)-> Any:
        """
        This function returns the content of the table, both indexesand vectors.
        It can return everything as a list of lists or as a numpy array (depending on the as_array parameter).
        """
        sql = f"SELECT * FROM {self.table_name}"
        result = self.db_connection.sqlite_conn.execute(sql).fetchall() 
        return array(result) if as_array else result 

    def create_row(self, pk:int, row_values:List[float]) -> None:
        """Add a new vector to the table. It requires the primary key."""
        assert len(row_values) == self.table_size, f"Wrong size. Array length={len(row_values)}, required length={self.table_size}"
        
        _insert_into_table_query = f"""INSERT INTO {self.table_name} VALUES (
            {pk}, 
            {array2string(array(row_values), separator=', ')[1 : -1]}
        );"""
        self.db_connection.write_on_db(_insert_into_table_query)

    def update_row(self, pk:int, row_values:List[Any]):
        """Update a vector in the table. It requires the primary key."""
        assert len(row_values) == self.table_size, f"Wrong size. Array length={len(row_values)}, required length={self.table_size}"
        
        _insert_into_table_query = f"""UPDATE {self.table_name} SET 
            {str(
                    ", ".join([ f"{self.table_name}_val_{str(i)} = {row_values[i]} " for i in range(self.table_size) ])
            )}
            WHERE id_{self.table_name} = {pk}
        ;"""
        self.db_connection.write_on_db(_insert_into_table_query)

    def delete_row(self, pk:int)-> bool:
        """Delete a vector from the table."""
        delete_cursor = self.db_connection.sqlite_conn.execute(f"DELETE FROM {self.table_name} WHERE id_{self.table_name} = {pk}")
        return delete_cursor.rowcount == 1

    def get_row(self, pk:int)-> List[Any]:
        """Get a vector from the table."""
        select_cursor = self.db_connection.sqlite_conn.execute(f"SELECT * FROM {self.table_name} WHERE id_{self.table_name} = {pk}")
        return select_cursor.fetchone()[1:]


