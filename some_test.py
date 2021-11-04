
import user_utils
from time import sleep
from pyinstrument import Profiler
import random
import cProfile
from pstats import Stats, SortKey

def test_user_creation():

    vum = user_utils.VectorUserManager("teo")
    vum._destruct()




