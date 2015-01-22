### Aggregation Function

Combines a set of time series into a single time series.
A conceptual measurement such as cpu usage or requests per second. A metric will usually end up getting reported as a set of time series, for example a cpu usage time series for each node.  

### Consolidation Function

Converts a time series from a primary step size to a larger step size that is a multiple of the primary. For example, if you have a time series with data points each minute and you want a time series with data points every 5 minutes.

In most cases you won't use a consolidation function directly. The system will automatically apply consolidation to ensure there is at least one pixel per datapoint on the rendered images.

### NaN Aware Operations

This typically refers to how binary operations treat NaN values in the data. A NaN value means that the value is unknown for a given interval. NaN aware operations will keep the value as NaN if both sides are NaN, but treat it as a special value such as 0.0 otherwise. This is useful to avoid gaps in the graph if a subset of time series used in an aggregate have gaps in data.

| *lhs* | *rhs* | *result* |
|-------|-------|----------|
| NaN | NaN | op(NaN, NaN) |
| v1 | NaN | op(v1, 0.0) | 
| NaN | v2 | op(0.0, v2) | 
| v1 | v2 | op(v1, v2) | 

### Naming Conventions

See [naming conventions](https://github.com/Netflix/spectator/wiki/Conventions).

### Normalization

In Atlas this usually refers to normalizing data points to step boundaries.

### Time Series

Metadata plus a set of data points reported at a regular interval. In Atlas the metadata is a set of string key value pairs called tags. A data point is a pair consisting of a timestamp and value. For Atlas the value is typically a double. 



