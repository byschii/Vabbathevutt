import sqlite3
from sqlite3.dbapi2 import Connection
from typing import Any, List, Literal, Optional
from pathlib import Path
from os      import remove  as remove_file
from os.path import exists  as file_exists
from annoy import AnnoyIndex
from numpy import array

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
        self.db_connection.write_on_db(self._get_table_creation_query())

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

    def dump_table(self, as_array:Optional[bool]=True)-> List[Any]:
        sql = f"SELECT * FROM {self.table_name}"
        result = self.db_connection.sqlite_conn.execute(sql).fetchall() 
        return array(result) if as_array else result 

    def insert_row(self, pk:int, row_values:List[Any]):
        assert len(row_values) == self.table_size, f"Wrong size. Array length={len(row_values)}, required length={self.table_size}"
        _INSERT_INTO_TABLE = f"""INSERT INTO {self.table_name} VALUES (
            {pk}, 
            {str(row_values)[1 : -1]}
        );"""
        self.db_connection.write_on_db(_INSERT_INTO_TABLE)

    def delete_row(self, pk:int)-> bool:
        delete_cursor = self.db_connection.sqlite_conn.execute(f"DELETE FROM {self.table_name} WHERE id_{self.table_name} = {pk}")
        return delete_cursor.rowcount == 1

    def get_row(self, pk:int)-> List[Any]:
        select_cursor = self.db_connection.sqlite_conn.execute(f"SELECT * FROM {self.table_name} WHERE id_{self.table_name} = {pk}")
        return select_cursor.fetchone()[1:]


VectorMetrics = Literal['angular', 'euclidean', 'hamming', 'dot']

class VectorIndex:
    def __init__(self, index_file_name:Path, num_dimensions:int, vector_distance_metric:VectorMetrics="angular", initial_vectors:Optional[List[List[float]]]=None ):
        assert index_file_name.suffix == ".idx", "Not a valid db file name ({index_file_name})" 
        self.index_file_name = index_file_name
        self.num_dimensions = num_dimensions
        self.vector_distance_metric:Literal['angular', 'euclidean', 'hamming', 'dot'] = vector_distance_metric

        self.vector_index = self._init_index()# if initial_vectors is None else self.update_index(initial_vectors)

    def _detach_index(self):
        self.vector_index.unload()
        if file_exists(self.index_file_name):
            remove_file(self.index_file_name)

    def _init_index(self) -> AnnoyIndex:
        _vi = AnnoyIndex(self.num_dimensions, self.vector_distance_metric)
        if file_exists(self.index_file_name):
            self.vector_index.load(str(self.index_file_name))
        return _vi

    def _update_index(self, data:List[List[float]])-> AnnoyIndex:
        np_data = array(data)
        return self.update_index(np_data[:, 0], np_data[:,1:])

    def update_index(self, indexes:List[int], vectors:List[List[float]], and_save:Optional[bool]=False)-> AnnoyIndex:
        self._detach_index()
        _vi = self._init_index()

        for index, vect in zip(indexes, vectors):
            self.vector_index.add_item(index, list(vect) )

        self.vector_index.build(max(1, int( len(indexes)**0.3 )))
        if and_save:
            self.vector_index.save(str(self.index_file_name))

        return _vi




db = DbManager(Path("./tmpdb.db"))



