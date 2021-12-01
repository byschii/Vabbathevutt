
from test_utils import timeit_context, timer
import time


import numpy as np

from vector_space import VectorSpace




def vector_insertion_speed_test(test_name, max_time):

    # creates 100 arrays of size 100
    arrays_to_insert = np.random.rand(600, 100)
    vs = VectorSpace("teo", 100)

    with timer(max_time, test_name) as t:
        for array in arrays_to_insert:
            vs.insert_vector(array)

    #prints the number of partitions
    print("Number of partitions: ", len(vs.spaces))
    vs.destroy()

    return t._is_ok()


