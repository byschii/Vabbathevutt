
from typing import Any, List, Literal, Optional, Union
from pathlib import Path
from os      import sep    as os_separator
from os      import remove as remove_file
from os.path import exists as file_exists

from annoy import AnnoyIndex
import numpy as np

from db_utils import *

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
        """ it deletes the index """
        self.vector_index.unload()
        if file_exists(self.index_file_name):
            remove_file(self.index_file_name)

    def _init_index(self) -> AnnoyIndex:
        _vi = AnnoyIndex(self.num_dimensions, self.vector_distance_metric)
        if file_exists(self.index_file_name):
            self.vector_index.load(str(self.index_file_name))
        return _vi

    def _update_index(self, data:List[List[float]])-> AnnoyIndex:
        np_data = np.array(data)
        return self.update_index(np_data[:, 0].astype(np.int), np_data[:,1:])

    def update_index(self, indexes:List[int], vectors:List[List[float]], and_save:Optional[bool]=False)-> AnnoyIndex:
        self._detach_index()
        self.vector_index = self._init_index()

        for index, vect in zip(indexes, vectors):
            self.vector_index.add_item(index, vect )

        self.vector_index.build(max(1, int( len(indexes)**self.tree_count_exponential )))
        if and_save:
            self.vector_index.save(str(self.index_file_name))

        return self.vector_index

    def get_nearest_vectors(self, ref:Union[int,List[float]], top_n:int, include_distances:bool=False ):
        if isinstance(ref, list):
            return self.vector_index.get_nns_by_vector(ref, top_n, include_distances)
        elif isinstance(ref, int):
            return self.vector_index.get_nns_by_item(ref, top_n, include_distances)
        else:
            raise Exception("Please, provide an index or a vector, got {ref}.")





class VectorSpace:
    def __init__(self, db_connection:DbManager, space_name:str, dimensions:int, description:Optional[str], max_unsynched_vectors:int=0):
        self.th = TableHandler(db_connection, space_name.split(os_separator)[-1], dimensions)
        self.vi = VectorIndex(Path(f"{space_name}.idx"), dimensions)
        self.description = description
        self.not_synched_vectors:int = 0
        self.max_unsynched_vectors:int = max_unsynched_vectors

    def insert_vector(self, vector:List[float], index:Optional[int]=None, force_update:bool=False ) -> int:
        """
        inserisce un nuovo vettore nello spazio
        se necessario, rebuilda l'index

        se non viene dato un indice, viene creato e ritornato
        """
        if index is None:
            new_index = self.th._get_num_rows_in_table() + 1
            self.th.create_row(new_index, vector)
            index = new_index
        else:
            self.th.create_row(index, vector)
        
        self._maybe_sync(1, force_update)
        return index 

    def _maybe_sync(self, weight_of_update:int, force_update:bool=False):
        self.not_synched_vectors += weight_of_update
        if (self.not_synched_vectors >= self.max_unsynched_vectors) or force_update:
            self.vi._update_index(self.th.dump_table())
            self.not_synched_vectors = 0

    def update_vector(self, vector:List[float], index:int ):
        """ aggiorna un vettore """
        self.th.update_row(index, vector)
        self._maybe_sync(1)

    def remove_vector(self, index:int):
        """ cancella un vettore """
        self.th.delete_row(index)
        self._maybe_sync(2)

    def get_space(self, with_index:bool=True) -> List[List[Any]]:
        """ ritorna tutto lo spazio vettoriale """
        space = self.th.dump_table()
        return space if with_index else space[:, 1:]

    def get_vector(self, index:int) -> List[Any]:
        """ ritorna un vettore in particolare """
        return self.th.get_row(index)

    def get_similar_vector(self, ref:Union[int,List[float]], top_n:int, include_distances:bool=False):
        """ return similar vector, veri fast (1ms)"""
        return self.vi.get_nearest_vectors(ref, top_n, include_distances)

    def _delete_vector_space(self):
        self.th._drop()
        self.vi._detach_index()



