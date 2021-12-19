

from pathlib import Path, PureWindowsPath
from db_manager import DbManager
from test_utils import timer

import numpy as np
from vector_index import VectorIndex

from vector_space import VectorSpace

# import plotext as plt

from vector_space_partition import VectorSpacePartition


def vector_insertion_speed_test(test_name, max_time):

    num_of_arrays = 1000
    array_dims = 100
    arrays_to_insert = np.random.rand(num_of_arrays, array_dims)

    EXPECTED_INSERTION_TIME = 0.3 * np.e
    print(EXPECTED_INSERTION_TIME)

    vs = VectorSpace("teo", array_dims, EXPECTED_INSERTION_TIME)

    insertion_times = []
    with timer(123) as global_timer:
        for array in arrays_to_insert:
            with timer(max_time) as t:
                vs.insert_vector(array)
            # print(t.elapsed * 1000)
            insertion_times.append(t.elapsed)


    print(f"""
    ---
    Total time: {global_timer.elapsed:.3f} seconds
    Average insertion time: {np.array(insertion_times).mean():.3f} seconds
    Median insertion time: {np.median(np.array(insertion_times)):.3f} seconds
    Max insertion time: {np.max(np.array(insertion_times)):.3f} seconds
    Number of partitions {len(vs.spaces)}
    ---
    """)


    """
    plt.clp()
    plt.scatter(insertion_times, )
    plt.title("Scatter Plot")
    plt.show()
    """

    # vs.destroy()

    return True



def vector_similarity_speed_test(test_name, max_time):

    num_of_arrays = 1700
    array_dims = 100
    arrays_to_insert = np.random.randn(num_of_arrays, array_dims)

    EXPECTED_INSERTION_TIME = 0.02

    vs = VectorSpace("teo", array_dims, EXPECTED_INSERTION_TIME)
    
    # arrays insertion
    for array in arrays_to_insert:
        vs.insert_vector(array)


    print(f"""
    ---
    Number of partitions {len(vs.spaces)}
    ---
    """)


    # average distance of retrived vectors    
    for _ in range(2):
        x = vs.get_similar_vectors(np.random.randn(array_dims), 3, False)
        print(x)
    


    
    vs.destroy()
    return True


vector_similarity_speed_test(vector_insertion_speed_test.__name__, 10)

# vector_insertion_speed_test(vector_insertion_speed_test.__name__, 10)





