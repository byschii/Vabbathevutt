

from pathlib import Path
from db_manager import DbManager
from test_utils import timer

import numpy as np

from vector_space import VectorSpace

import plotext as plt

from vector_space_partition import VectorSpacePartition


def vector_insertion_speed_test(test_name, max_time):

    num_of_arrays = 1200
    array_dims = 100
    arrays_to_insert = np.random.rand(num_of_arrays, array_dims)

    EXPECTED_INSERTION_TIME = 0.03

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
    {global_timer.elapsed / num_of_arrays:.3f} per insertion
    Average insertion time: {np.array(insertion_times).mean():.3f} seconds
    Median insertion time: {np.median(np.array(insertion_times)):.3f} seconds
    Number of partitions {len(vs.spaces)}
    ---
    """)



    plt.clp()
    plt.scatter(insertion_times, )
    plt.title("Scatter Plot")
    plt.show()

    vs.destroy()

    return True



def vector_similarity_speed_test(test_name, max_time):
    name = 'dd'
    dbc =  DbManager(Path(name + ".db"))
    vsp = VectorSpacePartition(dbc, name, 100)
    

    for _ in range(99):
        vsp.insert_vector(np.random.rand(100))

    i = vsp.get_similar_vectors(
        3,#np.random.rand(100),
        3
    )
    print(i)

vector_similarity_speed_test(vector_insertion_speed_test.__name__, 10)

# vector_insertion_speed_test(vector_insertion_speed_test.__name__, 10)


