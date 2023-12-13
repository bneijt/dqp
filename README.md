Disk Queue Processing
=====================


This library makes it easy to serialize Python to disk using msgpack.

The library contains two modules:

- `dqp.disk_cache`: to easily read/write data to disk and support caching iterators to disk.
- `dqp.disk_queue`: an approach to communicating lists of objects between runs with index/offset metadata.
