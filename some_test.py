
from test_utils import timeit_context, timer
import random


import numpy as np

from vector_space import VectorSpace





def vector_insertion_speed_test(test_name, max_time):

    # creates 100 arrays of size 100
    arrays_to_insert = np.random.rand(100, 100)
    vs = VectorSpace("teo.db", 100)

    with timer(max_time, test_name) as t:
        for array in arrays_to_insert:
            vs.insert_vector(array)

    return t._is_ok()

    
vector_insertion_speed_test(vector_insertion_speed_test.__name__, 10)





