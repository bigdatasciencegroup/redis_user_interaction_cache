# Redis User Interaction Cache

Simple example of a Redis based interaction cache to inject
a state into a scoring request for a stateful DS model.
Read more about the use of this in my blog posts:

1. [Rendezvous Architecture](https://towardsdatascience.com/rendezvous-architecture-for-data-science-in-production-79c4d48f12b)
1. [Introduction to Recommendation Engines](https://towardsdatascience.com/how-to-build-a-recommendation-engine-quick-and-simple-aec8c71a823e)
1. [Advanced Use-Cases for Recommendation Engines]()

Look at the unittests and docs for an explanation of how to use the cache.

This is NOT production ready code. 

## Dev
### Redis Container

Non-root container. Allow access to local folder:
```
sudo chown -R 1001:1001 redis-data/
```