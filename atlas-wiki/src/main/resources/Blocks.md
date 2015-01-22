The values for a time series in Atlas are stored as a set of blocks. A typical block size is 1-hour with one datapoint per minute. All block implementations must provide constant time access to the value for a given datapoint.

## Implementations

* `T`: size of a time stamp.
* `F`: size of a floating point value. 8 bytes for double and 4 bytes for float.
* `N`: number of datapoints per block.

### Array

The simplest implementation is just start time and an array with the values. This implementation is typically used for the data as it is flowing in and changing frequently. The minimum space required is `T + F * N`.

### Constant

If all values for a given block are the same the constant type can be used. The minimum space required is `T + S + F` where `S` is the size of the length param.

### Sparse

This is a simple compressed block format that is kind of like RLE, but allows for constant time access to values. The data is stored in two arrays, `indexes` and `values`. The `indexes` array is an array of bytes and has size `N`. Each position stores an index to the `values` array for the actual floating point value. This type is used when the number of distinct values for the block is small. Common values NaN, 0, and 1 are special cased to use a negative index so they do not actually need to be stored. The minimum space required is `T + N + F * V` where `V` is the number of values that need to be stored.

### Rollup

## Metrics

### atlas.mem.blockCount

Number of blocks currently in memory.

**Dimensions:**
* `type`: type of block

### atlas.mem.blockSize

Number of bytes currently in memory to represent blocks.

**Dimensions:**
* `type`: type of block