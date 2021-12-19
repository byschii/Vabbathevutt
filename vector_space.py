
from typing import Any, List, Optional
from pathlib import Path

from time import monotonic as timing
import numpy as np

from db_manager    import DbManager
from vector_space_partition import VectorSpacePartition, VectorSpacePartitionStats




class VectorSpace:
    """This class orchestrates multilpe VectorSpacePartition objects"""
    def __init__(self, name, dimensions:int, insertion_speed:float = 0.075, rebalance_probs:float = 0.65) -> None:
        """
        Creates a new vector space
        parameters:
            name: name of the vector space
            dimensions: number of dimensions of the vectors
            insertion_speed: a target speed for the vector insertion in seconds
            rebalance_probs: a number to rebalance the probability of a given partition to be choose as the insertion target
        """
        self.name = name
        self.dimensions:int = dimensions
        self.db_connection = DbManager(Path(name + ".db"))
        self.spaces: List[VectorSpacePartitionStats] = []
        self.max_insert_time = insertion_speed # seconds
        self.rebalance_probs = rebalance_probs
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

    def get_similar_vectors(self, ref:List[float], top_n:int, include_distances:bool=False):
        """return closest vectors to the given one"""
        similars = []
        _partition_indices, _partition_distances = None, None
        for space in self.spaces:
            _partition_indices, _partition_distances = space.vector_space_partition.get_similar_vectors(
                ref, space.vector_space_size()//15 + 1, True
            )
            assert isinstance(_partition_indices, list) and isinstance(_partition_distances, list)
            similars += [
                ( _pi, _pd )
                for _pi, _pd in zip(_partition_indices, _partition_distances)
            ]

        # filter similar vectors by lowest distance
        similars = sorted(similars, key=lambda x: x[1])
        similars = np.array(similars)[:top_n]
        return similars.T if include_distances else similars[:,0]

        
    def insert_vector(self, vector:List[float], pk:Optional[int]=None, force_update:bool=False) -> None:
        """
        Inserts a vector in a random partition.
        If the insertion is too slow, it will create a new partition (smaller, so faster to update)
        """
        start = timing()
        random_partition_index = 0
        if len(self.spaces)>1:
            spaces_size = np.array([vsps.vector_space_size() for vsps in self.spaces])
            rp = lambda p,m: p * (1+self.rebalance_probs) if p > m else p * (1-self.rebalance_probs)
            spaces_size = np.array([rp(s, spaces_size.mean()) for s in spaces_size])
            probs = (1-spaces_size / np.sum(spaces_size)) / (spaces_size.size-1)
            random_partition_index = np.random.choice(
                np.arange(spaces_size.size),
                p = probs
            )
            # random_partition_index = np.random.choice( np.arange(spaces_size.size))
        
        pk = max((
            max(s.pks_in_vector_space_partition) if len(s.pks_in_vector_space_partition)>0 else 0
            for s in self.spaces
        )) + 1 
        new_pk = self.spaces[random_partition_index].vector_space_partition.insert_vector(vector, pk, force_update)
        self.spaces[random_partition_index].pks_in_vector_space_partition.add(new_pk)

        if timing()-start > self.max_insert_time:
            self.create_partition()

    def destroy(self) -> None:
        """
        Destroys every partition from the space
        and deletes the database file
        """
        for s in self.spaces:
            s.vector_space_partition._delete_vector_space()

        self.db_connection._detach_sqlite()

