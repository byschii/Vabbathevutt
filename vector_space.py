
from typing import Any, List, Optional
from pathlib import Path

from time import monotonic as timing
import numpy as np

from db_manager    import DbManager
from vector_space_partition import VectorSpacePartition, VectorSpacePartitionStats




class VectorSpace:
    """This class orchestrates multilpe VectorSpacePartition objects"""
    def __init__(self, name, dimensions:int, insertion_speed:float = 0.075) -> None:
        self.name = name
        self.dimensions:int = dimensions
        self.db_connection = DbManager(Path(name + ".db"))
        self.spaces: List[VectorSpacePartitionStats] = []
        self.max_insert_time = insertion_speed # seconds
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
        """return closest vectors to the given one"""
        similars = []
        for space in self.spaces:
            s = space.vector_space_partition.get_similar_vectors(ref, top_n//len(self.spaces)+1, False)
            print(s, ref)
            similars += s 

        # filter similar vectors by lowest distance
        #similars = sorted(similars, key=lambda x: x[0])
        #similars = similars[:top_n]
        return similars
        
    def insert_vector(self, vector:List[float], index:Optional[int]=None, force_update:bool=False) -> None:
        """
        Inserts a vector in a random partition.
        If the insertion is too slow, it will create a new partition (smaller, so faster to update)
        """
        start = timing()
        random_partition_index = 0
        if len(self.spaces)>1:
            spaces_size = np.array([vsps.vector_space_size for vsps in self.spaces])
            probs = ( 1 - spaces_size / np.sum(spaces_size)) / ( spaces_size.size -1)
            random_partition_index = np.random.choice(
                np.arange(spaces_size.size),
                p = probs
            )
        new_pk = self.spaces[random_partition_index].vector_space_partition.insert_vector(vector, index, force_update)
        self.spaces[random_partition_index].vector_space_size += 1
        self.spaces[random_partition_index].pks_in_vector_space_partition.add(new_pk)

        if timing()-start > self.max_insert_time:
            self.create_partition()

    def destroy(self) -> None:
        """
        Destroys every partition from the space
        """
        for s in self.spaces:
            s.vector_space_partition._delete_vector_space()

        self.db_connection._detach_sqlite()

