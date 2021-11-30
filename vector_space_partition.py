
from typing import Any, List, Optional, Union
from pathlib import Path
from os      import sep    as os_separator
import time
import numpy as np

from table_handler import TableHandler
from vector_index  import VectorIndex
from db_manager    import DbManager


class VectorSpacePartition:
    """
    This class is used to coordinate both the vector store (with TableHandler) and the vector index (with VectorIndex)
    It requires a DBManager object to be able to create the table and a name to reference db, index, and tables.
    It is usefull in two ways:
        - can avoid to update the index for every vector insertion with in increase the speed
        - it is easy to use in conjunction with other instances with in increase the speed
    The slowest part is always keeping the index up to date.
    """
    def __init__(self, db_connection:DbManager, space_name:str, dimensions:int, max_unsynched_vectors:int=0):
        self.th = TableHandler(db_connection, space_name.split(os_separator)[-1], dimensions)
        self.vi = VectorIndex(Path(f"{space_name}.idx"), dimensions)
        self.not_synched_vectors:int = 0
        self.max_unsynched_vectors:int = max_unsynched_vectors

        self.DELETIION_WEIGHT:int = 2
        self.INSERTION_WEIGHT:int = 1
        self.UPDATE_WEIGHT:int = 1

    def insert_vector(self, vector:List[float], pk:Optional[int]=None, force_update:bool=False ) -> int:
        """
        Insert a vector in the space. If necessary, the index will be updated.
        If the index (vector pk) is not provided, it will be automatically generated.
        """
        if pk is None:
            new_pk = self.th._get_num_rows_in_table() + 1
            self.th.create_row(new_pk, vector)
            pk = new_pk
        else:
            self.th.create_row(pk, vector)
        
        self._maybe_sync(self.INSERTION_WEIGHT, force_update)
        return pk 

    def _maybe_sync(self, weight_of_update:int, force_update:bool=False):
        """
        This method is used to decide whether to update the index or not.
        It is called every time a vector is inserted, deleted or updated.
        
        The decision is made based on the number of unsynched vectors, which is updated with a weight provided by the caller.
        Insertion weight is 1, deletion weight is 2.
        """
        self.not_synched_vectors += weight_of_update
        if (self.not_synched_vectors >= self.max_unsynched_vectors) or force_update:
            self.vi._update_index(self.th.dump_table())
            self.not_synched_vectors = 0

    def update_vector(self, vector:List[float], pk:int ):
        """Updates a vector in the space via the TableHandler"""
        self.th.update_row(pk, vector)
        self._maybe_sync(self.UPDATE_WEIGHT)

    def remove_vector(self, pk:int):
        """Remove a vector in the space via the TableHandler"""
        self.th.delete_row(pk)
        self._maybe_sync(self.DELETIION_WEIGHT)

    def get_space(self, with_pk:bool=True) -> List[List[Any]]:
        """This function returns the entire space as a list of pk+vectors"""
        space = self.th.dump_table()
        return space if with_pk else space[:, 1:]

    def get_vector(self, pk:int) -> List[Any]:
        """Returns a specific vector"""
        return self.th.get_row(pk)

    def get_similar_vector(self, ref:Union[int, List[float]], top_n:int, include_distances:bool=False):
        """
        Return vectors similar to 'ref'.
        Tipically very fast (1ms).

        This function can be usefull to get the pk of a vector (which may have been inserted without storing the index)
        """
        return self.vi.get_nearest_vectors(ref, top_n, include_distances)

    def _delete_vector_space(self):
        """
        This function deletes the partition with every vector contained in it.
        The TableHandler is dropped and the VectorIndex is detached.
        """
        self.th._drop()
        self.vi._detach_index()


class VectorSpacePartitionStats:
    """This class is just to help with the handling of multiple VectorSpacePartition"""
    def __init__(self, vsp: VectorSpacePartition):
        self.vector_space_partition = vsp 
        self.vector_space_size = 0
        self.pks_in_vector_space_partition = set()
