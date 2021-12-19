# import numpy just to create example vectors
import numpy as np
# import the VectorSpace class
from vector_space import VectorSpace

# instantiate a space (with a name and the size of the vectors that will be inserted)
vs = VectorSpace("my_fat_space", dimensions=42)

# insert some vector
vs.insert_vector(np.random.rand(42))
# you can also insert vector with a key to reference them in you system
vs.insert_vector(np.random.rand(42), pk=2)

# then you can ask the space for similar vectors
similar_vector_indices = vs.get_similar_vectors(
    np.random.rand(42), top_n=2, include_distances=False
)
# and get the closest stored vector in the db
print(
    vs.get_vector(pk=similar_vector_indices[0])
)

# eventually, you can delete everything with
vs.destroy()