
from test_utils import timeit_context, timer
import random
import vector_utils
import db_utils
import numpy as np

"""
def user_creation_and_destruction(test_name, max_time):

    with timer(max_time, test_name) as t:
        vum = vector_utils.VectorSpace("teo")
        vum._destruct()
    return t._is_ok()

user_creation_and_destruction(f"test_{user_creation_and_destruction.__name__}", 10)
"""


sizes = [22,3,12,44,1]
indexes = [0,1,2,3,4]

probs = list(map(lambda x:1-x/sum(sizes), sizes ))

print(probs)
for i in range(10):
    print(np.random.choice(indexes, p=[0.1, 0.05, 0.05, 0.2, 0.4, 0.2]))




