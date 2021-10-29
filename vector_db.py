from typing import Literal
from annoy import AnnoyIndex
import sqlite3

from os      import remove  as remove_file
from os.path import exists  as file_exists
from math    import sqrt    as math_sqrt
from random  import randint, random

import time


class VectorDb:
	"""
	classe fatta per gestire solo dei vettori
	"""
	def __init__(self, file_name:str, array_length:int, vector_distance_metric:Literal['angular', 'euclidean', 'hamming', 'dot']="angular", not_in_index_toll:float=0.09 ):
		self.file_name           = file_name
		self.array_length        = array_length
		self.metric: Literal['angular', 'euclidean', 'hamming', 'dot']= vector_distance_metric
		self.not_in_index_toll   = not_in_index_toll
		self.db_file_name        = file_name+".db"
		self.db_table            = file_name+str(randint(1,99))
		self.index_file_name     = file_name+".ann"
		self.arrays_not_in_index = 0
		self.vector_index        = self._init_index()
		self.sqlite_conn         = self._init_sqlite()

	def _delete_db(self) -> None:
		self._detach_index()
		self._detach_sqlite()

	def _detach_sqlite(self):
		self.sqlite_conn.close()
		if file_exists(self.db_file_name):
			remove_file(self.db_file_name)

	def _detach_index(self):
		self.vector_index.unload()
		# devo cavare il file 
		if file_exists(self.index_file_name):
			remove_file(self.index_file_name)

	def _init_index(self):
		self.vector_index = AnnoyIndex(self.array_length, self.metric)
		if file_exists(self.index_file_name):
			self.vector_index.load(self.index_file_name)
		return self.vector_index

	def _init_sqlite(self):
		self.sqlite_conn = sqlite3.connect(self.db_file_name)
		_CREATE_TABLE = f"""CREATE TABLE IF NOT EXISTS {self.file_name} (
			id_{self.file_name} INTEGER PRIMARY KEY,
			{str(
				", ".join([
					f"{self.file_name}_val_{str(i)} REAL NOT NULL" for i in range(self.array_length)
				])
			)}
		);"""
		self._write_on_db(_CREATE_TABLE)
		return self.sqlite_conn

	def get_num_arrays_in_db(self):
		"""
		costa circa 0.01sec
		"""
		num_arrays = self.sqlite_conn.execute(f"""SELECT count(*) FROM {self.file_name}""")
		return num_arrays.fetchone()[0]


	def insert_new_vector(self, pk, array_val, force_update_index=False):
		assert len(array_val) == self.array_length, f"Wrong size. Array length = {len(array_val)}, required length = {self.array_length}"
		_INSERT_INTO_TABLE = f"""INSERT INTO {self.file_name} VALUES (
			{pk}, 
			{str(array_val)[1 : -1]}
		);"""
		self._write_on_db(_INSERT_INTO_TABLE)

		if force_update_index or (self.arrays_not_in_index / self.get_num_arrays_in_db()) > self.not_in_index_toll :
			self.update_vector_index()
		else:
			self.arrays_not_in_index += 1

	def update_vector_index(self):
		start_time = time.time()
		self._detach_index()
		self._init_index()
		
		self.arrays_not_in_index = 0
		arrays_in_db = self.sqlite_conn.execute(f"""SELECT * FROM {self.file_name}""").fetchall()
		for row in arrays_in_db:
			self.vector_index.add_item(
				row[0], row[1:]
			)

		self.vector_index.build(max(1, int( len(arrays_in_db)**0.3 )))
		self.vector_index.save(self.index_file_name)
		print(round(time.time() - start_time,3) , len(arrays_in_db) )





	def _write_on_db(self, sql):
		_cursor = self.sqlite_conn.cursor()
		_cursor.execute(sql)
		self.sqlite_conn.commit()



if __name__ == '__main__':
	x = VectorDb("faces", 128, not_in_index_toll=0.35)
	for i in range(5_000):
		x.insert_new_vector(
			i, [random() for _ in range(128)]
		)


	start_time = time.time()
	x.insert_new_vector(111_111, [random() for _ in range(128)], True ) 
	print(time.time() - start_time)

	#x._delete_db() #140
	









