# Planning

Before running an immudb database in production, it is important to properly plan the following:

* Compute resources (CPU)
* Disk space
* Configuration
* Backups
* Health Monitoring

### Compute Resources (CPU) <a href="#compute-resources" id="compute-resources"></a>

immudb was designed to have a stable memory/CPU footprint.

Memory is pre-allocated based on specified maximum concurrency, maximum number of entries per transaction, cache sizes, etc.

With the default settings, it's possible to stress immudb and memory usage should stay around 1.5GB (assuming low-sized values). Otherwise, memory will be needed to maintain the values within a transaction during commit time.

### Disk space and data location <a href="#disk-space-and-data-location" id="disk-space-and-data-location"></a>

immudb is an immutable database, this means all history is preserved and therefore disk usage is higher than a normal database.

Data is stored in the directory specified by the `dir` option. See the [Configuration Reference](../reference/configuration.md) for more details.
