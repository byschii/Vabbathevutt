
from typing import Any, List, Optional
from pathlib import Path

import time
import numpy as np

from db_manager    import DbManager
from vector_space_partition import VectorSpacePartition, VectorSpacePartitionStats



class VectorSpace:
    """This class orchestrates multilpe VectorSpacePartition objects"""
    def __init__(self, name, dimensions:int) -> None:
        self.name = name
        self.dimensions:int = dimensions
        self.db_connection = DbManager(Path(name))
        self.spaces: List[VectorSpacePartitionStats] = []
        self.max_insert_time = 5 # seconds
        self.create_partition()

    def create_partition(self, max_unsynched_vectors:int=0) -> None:
        """Creates a partition with a default name """
        self.spaces.append(
            VectorSpacePartitionStats(
                VectorSpacePartition(
                    self.db_connection,
                    f"{self.name}_{len(self.spaces)}",
                    self.dimensions,
                    max_unsynched_vectors=max_unsynched_vectors
                )
            )
        )

    def get_vector(self, pk:int) -> List[float]:
        """
        Returns a vector from the vector space
        """
        for space in self.spaces:
            if pk in space.pks_in_vector_space_partition:
                return space.vector_space_partition.get_vector(pk)
        raise ValueError(f"Vector with pk {pk} not found")

    def get_similar_vector(self, ref:List[float], top_n:int, include_distances:bool=False):
        """ return similar vector"""
        similars = []
        for space in self.spaces:
            similars += space.get_similar_vector(ref, top_n//len(self.spaces), include_distances)
        return similars
        
    def insert_vector(self, vector:List[float], index:Optional[int]=None, force_update:bool=False) -> None:
        """
        Inserts a vector in a random partition.
        If the insertion is too slow, it will create a new partition (smaller, so faster to update)
        """
        start = time.time()
        spaces_size = [vsps.vector_space_size for vsps in self.spaces]
        random_partition_index = np.random.choice(
            range(len(self.spaces)),
            p=list(map(lambda x:1-x/sum(spaces_size), spaces_size ))
        )
        new_pk = self.spaces[random_partition_index].vector_space_partition.insert_vector(vector, index, force_update)
        self.spaces[random_partition_index].vector_space_size += 1
        self.spaces[random_partition_index].pks_in_vector_space_partition.add(new_pk)
        if time.time()-start > self.max_insert_time:
            self.create_partition()

