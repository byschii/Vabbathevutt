
from typing import List
from test_utils import timeit_context, timer
import time

import numpy as np

from vector_space import VectorSpace




def vector_insertion_speed_test(test_name, max_time):

    # creates 100 arrays of size 100
    num_of_arrays = 15000
    array_dims = 128
    arrays_to_insert = np.random.rand(num_of_arrays, array_dims)
    vs = VectorSpace("teo", array_dims)

    times = []
    with timer(123) as global_timer:
        for array in arrays_to_insert:
            with timer(max_time) as t:
                vs.insert_vector(array)
            times.append(t.elapsed)
    print(f"Total time: {global_timer.elapsed / 1000 / 60} minutes")
    print(f"{global_timer.elapsed / num_of_arrays} per insertion")
    print("--")



    print("Number of partitions: ", len(vs.spaces))
    print("Average insertion time: ", np.mean(np.array(times)))
    vs.destroy()

    return True

vector_insertion_speed_test(vector_insertion_speed_test.__name__, 10)


