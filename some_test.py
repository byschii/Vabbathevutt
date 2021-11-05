
import user_utils
from test_utils import timeit_context, timer
import random

def user_creation_and_destruction(test_name, max_time):
    """tests for execution time of creation and destruction of user"""
    with timer(max_time, test_name) as t:
        vum = user_utils.VectorUserManager("teo")
        vum._destruct()
    return t._is_ok()


def vector_insertion(test_name:str, max_time:int):
    """
    tests for execution time of vector insertion without optimizations
    165 vettori => 0.4sec
    330 vettori => 1 sec
    660 vettori => 4.5 sec
    """
    # params
    N_VECTOR_TO_INSERT = 330
    VECTOR_DIM = 5
    vum = user_utils.VectorUserManager("teo")
    vum.add_space("smal_space", VECTOR_DIM)
    with timer(max_time, test_name) as t:
        for i in range(0, N_VECTOR_TO_INSERT):
            vum.vs["smal_space"].insert_vector(
                [random.random() for _ in range(0, VECTOR_DIM)]#, i
            )

    vum._destruct()
    return t._is_ok(), t.et


def vector_insertion_optimized(test_name:str, max_time:int):
    """
    tests for execution time of vector insertion with optimizations
    165 vettori => 0.03sec
    330 vettori => 0.1 sec
    660 vettori => 0.3 sec
    1000 vetteri => 0.7sec
    """
    # params
    N_VECTOR_TO_INSERT = 330
    VECTOR_DIM = 5
    MAX_UNSTNC_VEC = 15
    vum = user_utils.VectorUserManager("teo")
    vum.add_space("smal_space", VECTOR_DIM, max_unsynched_vectors=MAX_UNSTNC_VEC)
    with timer(max_time, test_name) as t:
        for i in range(0, N_VECTOR_TO_INSERT):
            vum.vs["smal_space"].insert_vector(
                [random.random() for _ in range(0, VECTOR_DIM)]#, i
            )

    vum._destruct()
    return t._is_ok(), t.et

def vector_finding(test_name:str, max_time:int):
    """
    tests for execution time of vector retrival
    """
    # params
    N_VECTOR_TO_INSERT = 330
    VECTOR_DIM = 5
    TIME_TO_RETRIVE = 20
    vum = user_utils.VectorUserManager("teo")
    vum.add_space("smal_space", VECTOR_DIM)
    for i in range(0, N_VECTOR_TO_INSERT):
        vum.vs["smal_space"].insert_vector(
            [random.random() for _ in range(0, VECTOR_DIM)]#, i
        )

    with timer(max_time, test_name) as t:
        for i in range(TIME_TO_RETRIVE):
            vum.vs["smal_space"].get_similar_vector(
                [random.random() for _ in range(0, VECTOR_DIM)],
                random.randint(0, N_VECTOR_TO_INSERT) - 6
            )

    vum._destruct()
    return t._is_ok(), t.et


user_creation_and_destruction(f"test_{user_creation_and_destruction.__name__}", 10)
vector_insertion(f"test_{vector_insertion.__name__}", 1050)
vector_insertion_optimized(f"test_{vector_insertion.__name__}", 330)
vector_finding(f"test_{vector_finding.__name__}", 10)

