# Vabbathevutt

## Description



## How to

## Under the hood

## Requirements

## To Do

- Make it a module on `pip`

- Make tests with an sql server
    - harder to handle (sqlite is much easier)
    - maybe faster

- Search similar vectors by minumum distance

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

## Notes
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


