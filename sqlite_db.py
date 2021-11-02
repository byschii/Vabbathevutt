import sqlite3
from sqlite3.dbapi2 import Connection
from typing import Any, List, Literal, Optional, Union
from pathlib import Path
from os      import remove  as remove_file
from os.path import exists  as file_exists
import typing
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
        """
        if table already exists
        it s not rebuilt
        """
        if not self._table_exists() or not self._get_num_rows_in_table() > 0:
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

    def _get_num_rows_in_table(self) -> int:
        """
        costa circa 0.01sec
        """
        num_arrays = self.db_connection.sqlite_conn.execute(f"""SELECT count(*) FROM {self.table_name}""")
        return num_arrays.fetchone()[0]

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
    def __init__(self, index_file_name:Path, num_dimensions:int, vector_distance_metric:VectorMetrics="angular", initial_vectors:Optional[List[List[float]]]=None, tree_count_exponential:float = 0.3 ):
        assert index_file_name.suffix == ".idx", "Not a valid db file name ({index_file_name})" 
        self.index_file_name = index_file_name
        self.num_dimensions = num_dimensions
        self.vector_distance_metric:Literal['angular', 'euclidean', 'hamming', 'dot'] = vector_distance_metric
        self.tree_count_exponential = tree_count_exponential

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

        self.vector_index.build(max(1, int( len(indexes)**self.tree_count_exponential )))
        if and_save:
            self.vector_index.save(str(self.index_file_name))

        return _vi

    def get_nearest_vectors(self, ref:Union[int,List[float]], top_n:int, include_distances:bool=False ):
        if isinstance(ref, list):
            return self.vector_index.get_nns_by_vector(ref, top_n, include_distances)
        elif isinstance(ref, int):
            return self.vector_index.get_nns_by_item(ref, top_n, include_distances)
        else:
            raise Exception("Please, provide an index or a vector, got {ref}.")


class VectorSpace:
    def __init__(self, db_connection:DbManager, space_name:str, dimensions:int):
        self.th = TableHandler(db_connection, space_name, dimensions)
        self.vi = VectorIndex(Path(f"{space_name}.idx"), dimensions)

    def add_vector(self, vector:List[float], index:Optional[int] ) -> Optional[int]:
        if index is None:
            new_index = self.th._get_num_rows_in_table() + 1
            self.th.insert_row(new_index, vector)
            return new_index
        else:
            self.th.insert_row(index, vector)
        

    def remove_vector(self, index:int):
        pass

    def get_space(self, with_index:bool=True):
        pass




"""
L' idea è di creare un db per utente
per ogni db creare tabelle per ogni spazio vettoriale

per ogni spazio anche un indice
"""

db = DbManager(Path("./tmpdb.db"))



