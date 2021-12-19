from typing import Any, List, Tuple, Literal, Optional, Union
from pathlib import Path
from os      import sep    as os_separator
from os      import remove as remove_file
from os.path import exists as file_exists
import time
from annoy import AnnoyIndex
import numpy as np

from table_handler import *

VectorMetrics = Literal['angular', 'euclidean', 'hamming', 'dot']
BoolToF = Literal[True, False]

class VectorIndex:
    """
    This class is a wrapper for the Annoy library.
    Containst an Annoy index and helps keeping it up to date when adding new vectors.
    This is done by saving the dimension of the vectors, the distance metric used and a name to eventually save the index.s
    """
    def __init__(self, index_file_name:Path, num_dimensions:int, vector_distance_metric:VectorMetrics="euclidean", initial_vectors:Optional[List[List[float]]]=None, tree_count_exponential:float = 0.3 ):
        assert index_file_name.suffix == ".idx", "Not a valid db file name ({index_file_name})" 
        self.index_file_name = index_file_name
        self.num_dimensions = num_dimensions
        self.vector_distance_metric:VectorMetrics = vector_distance_metric
        self.tree_count_exponential = tree_count_exponential

        self.vector_index = self._init_index()# if initial_vectors is None else self.update_index(initial_vectors)
        if initial_vectors is not None:
            self._update_index(initial_vectors)


    def _detach_index(self):
        """It deletes the index, and if it were store in a file, it deletes the file."""
        self.vector_index.unload()
        if file_exists(self.index_file_name):
            remove_file(self.index_file_name)

    def _init_index(self) -> AnnoyIndex:
        """
        This method creates the index and returns it.
        If the index is already in a file, it loads the index from the file.
        """
        _vi = AnnoyIndex(self.num_dimensions, self.vector_distance_metric)
        if file_exists(self.index_file_name):
            self.vector_index.load(str(self.index_file_name))
        return _vi

    def update_index(self, indexes:List[int], vectors:List[List[float]], and_save:Optional[bool]=False)-> AnnoyIndex:
        """
        Destroy and rebuild the index with the given vectors.
        Eventually, it saves the index in a file (default: False, because everything is also stored in ram).
        """
        self._detach_index()
        self.vector_index = self._init_index()

        for index, vect in zip(indexes, vectors):
            self.vector_index.add_item(index, vect )

        self.vector_index.build(max(1, int( len(indexes)**self.tree_count_exponential )))
        if and_save:
            self.vector_index.save(str(self.index_file_name))

        return self.vector_index

    def _update_index(self, data:List[List[float]])-> AnnoyIndex:
        """Another way to update the index. It is used when index and array are given together."""
        np_data = np.array(data)
        return self.update_index(np_data[:, 0].astype(np.int), np_data[:,1:])

    def get_nearest_vectors_indices(self,
        ref:Union[int,List[float]], top_n:int, include_distances:bool=False
        ) -> Union[ List[int], Tuple[List[int], List[float]] ]:
        """
        Return a list of the closes vectors rappresented as their indices during insertion.
        The reference vector, can be provided both as its index in the space or as a list of floats (if the vector has not been inserted).
        It is possible to return the distances too.

        I splitted the function in two because I found that the Annoy library typing requires a 'Literal[True]/Literal[False]
        """
        if include_distances:
            return self._get_nearest_vectors_indices_with_distancies(ref, top_n)
        else:
            return self._get_nearest_vectors_indices_without_distancies(ref, top_n)


    def _get_nearest_vectors_indices_with_distancies(self, ref:Union[int,List[float]], top_n:int) -> Tuple[List[int], List[float]]:
        if isinstance(ref, list) or isinstance(ref, np.ndarray):
            return  self.vector_index.get_nns_by_vector(ref, top_n, include_distances = True)
        elif isinstance(ref, int):
            return self.vector_index.get_nns_by_item(ref, top_n, include_distances = True)
        else:
            raise Exception(f"Please, provide an index or a vector, got {type(ref)}.")

    def _get_nearest_vectors_indices_without_distancies(self, ref:Union[int,List[float]], top_n:int) -> List[int]:
        if isinstance(ref, list) or isinstance(ref, np.ndarray):
            return  self.vector_index.get_nns_by_vector(ref, top_n, include_distances = False)
        elif isinstance(ref, int):
            return self.vector_index.get_nns_by_item(ref, top_n, include_distances = False)
        else:
            raise Exception(f"Please, provide an index or a vector, got {type(ref)}.")
    

    def get_vectors_from_indices(self, indices:Union[int, List[int]] )-> Union[List[float], List[List[float]]]:
        """
        Converts every index in the corresponding vector.
        """
        if isinstance(indices, int):
            return self.vector_index.get_item_vector(indices)
        return np.array([self.vector_index.get_item_vector(index) for index in indices])

    @staticmethod
    def get_vector_distance(vector_1:List[float], vector_2:List[float], vector_distance_metric:VectorMetrics="euclidean",)-> float:
        """
        Return the distance between two vectors.
        """    

        _vi = AnnoyIndex(len(vector_1), vector_distance_metric)
        _vi.add_item(0, vector_1)
        _vi.add_item(1, vector_2)
        return _vi.get_distance(0, 1)

    