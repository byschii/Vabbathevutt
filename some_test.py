

from test_utils import timer

import numpy as np

from vector_space import VectorSpace

import plotext as plt


def vector_insertion_speed_test(test_name, max_time):

    num_of_arrays = 1500
    array_dims = 100
    arrays_to_insert = np.random.rand(num_of_arrays, array_dims)

    EXPECTED_INSERTION_TIME = 0.04

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

vector_insertion_speed_test(vector_insertion_speed_test.__name__, 10)


