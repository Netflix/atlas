Examples for using multiple Y-axes:

* [Explicit](#explicit)
* [Explicit Bounds](#explicit-bounds)
* [Axis Per Line](#axis-per-line)

## Explicit

By default all lines will go on axis 0, the one on the left side. A different axis can be
specified using the [:axis](style-axis) operation.

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,42,1,:axis

## Explicit Bounds

By default all axes will pick up [axis settings](Graph#y-axis) with no qualifier:

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,42,1,:axis&l=0

Bounds and other axis settings can be set per axis, e.g., this graph moves the constant line for
42 to a separate axis and sets the lower bound to 0:

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,42,1,:axis&l.1=0

## Axis Per Line

There is a convenience operation to plot each line on a separate axis.

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-p,:re,:and,(,nf.cluster,),:by&axis_per_line=1

If there are too many lines and it would be over the max Y-axis limit, then a warning will be shown:

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by&axis_per_line=1
