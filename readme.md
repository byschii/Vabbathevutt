# Vabbathevutt

## Description
*Vabbathevutt* is a small, hackable, (and slow) library that laverage [Annoy](https://github.com/spotify/annoy) and [SQLite](https://docs.python.org/3/library/sqlite3.html) to store vectors and do similarity search.

*Vabbathevutt* is very early stage, contatins bug and is not seriously tested.

### Why?
Because I was into a project involving lots of image embedding that needed to be stored. 

I knew about Pinecone (also mentioned at the end of this readme) but I didn't want to learn a new module, make yet another account and rely on an external service (over the internet)... so I simply decided to solve the problem my own way.

## How to
First: install the requirements and download the library (eg. with `git clone`).

Then:
```python
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

# eventually, you can delete everything
vs.destroy()
```

## Under the hood
When you istantiate a `VectorSpace`, the library creates a `DbManager` (which handles a connection to an SQLite database stored on a file called as the space) and a list of `VectorSpacePartitionStats`.

`VectorSpacePartitionStats` just contains a `VectorSpacePartition` and some information on it (which are used to make decision during operations on the `VectorSpace`).

The `VectorSpacePartition` contains a `TableHandler` and a `VectorIndex`. Those are used respectively to store the vectors inserted in the space and to search for similar vectors in the space using the Annoy module.
When a vector is inserted, it is stored in a table on the SQLite database and the index gets updated (by deleting it and rebuilding it).

### More things you can controll
- The `VectorSpace` can be created with a `insertion_speed` parameter. Every insertion is timed and if the time taken is greater than `insertion_speed` than a new `VectorSpacePartitionStats` is instantiated and on next insertion it is very likely to be used as target for new vectors (since the new instance is smaler, it will take less time to keep the `VectorIndex` updated).

- Also, the `VectorSpacePartition` handles `max_unsynched_vectors`. It allows you to store vector in the database without updating the index. It defaults to 0. Every operation in the `VectorSpacePartition` that modifies the space (`insert_vector`, `update_vector`, `remove_vector`) increments a counter which eventually signals a `VectorIndex` update.

- You can also change the distance Annoy uses to calculate similarity. In the `VectorIndex` class there's `vector_distance_metric` which can be set to `Literal['angular', 'euclidean', 'hamming', 'dot']` (`euclidean` is default).

There is also a little `timer` class to be used in the `with` construcotr to time operations.

## Requirements
To run the library you need 
- [numpy](https://numpy.org/doc/stable/user/whatisnumpy.html) (you can run `pip install --user numpy`)
- [Annoy](https://github.com/spotify/annoy) (can install with `pip install --user annoy`)
    - On Windows, you might need to install [C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools)

## To Do
As written before, this library is very early stage and i have lots of ideas on what to add.

- Make it a module on `pip`

- Add `requirements.txt` to rapidly install dependencies

- Better unit testing

- Store multipla spaces in the same db

- Make tests with an sql server
    - harder to handle (sqlite is much easier)
    - maybe faster

- Search similar vectors by minumum distance
    - https://scikit-learn.org/stable/modules/generated/sklearn.neighbors.RadiusNeighborsClassifier.html

- Augment parameters you can set in every component
    - number of not synched vector
    - metric for distance

- Batch insertion
    - inserting many vector at once will be much optimized

- Search vector-to-pk
    - might be slower, but it s ok

- Better and more consistent naming

- Mate test with some kind of clustring in partition
    - to optimize search

- Update to HNSW
    - https://arxiv.org/ftp/arxiv/papers/1603/1603.09320.pdf
    - https://github.com/nmslib/hnswlib



## Other Notes
#### Plot metrics
Do you want to plot some metric, like the size of vector space partition or the insertion time?
I found `plotext` usefull enought.

```python
import plotext as plt

plt.clp()
plt.scatter( [s.vector_space_size() for s in vs.spaces] )
plt.title("Scatter Plot")
plt.show()
```

#### Valuable Alternatives
- [Milvus](https://milvus.io)
- [Pinecone](https://www.pinecone.io)
- [Vespa](https://vespa.ai)
- [Weaviate](https://www.semi.technology/developers/weaviate/current)
- [Vald](https://vald.vdaas.org)
- [Drant](https://qdrant.tech)


