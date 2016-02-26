> [[Home]] ▸ Examples

Examples for using multiple Y-axes:

* [Explicit](#explicit)
* [Explicit Bounds](#explicit-bounds)
* [Axis Per Line](#axis-per-line)
* [Palettes](#palettes)

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

## Palettes

By default, all lines on a given axis will get the same color. That color will also get used to
render the axis. The intention is to make it easy to understand which axis a line is associated
with and in an image dynamic clues like hover cannot be used. Generally it is recommended to
only have one line per axis when using multi-Y. Example:

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by,minuteOfHour,:time,1,:axis&l=0

Though it should be avoided, a color palette can be specified for a specific axis so lines will
get different colors, e.g., the graph below sets the palette for each axis to use shades of
red/blue:

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by,:stack,minuteOfHour,:time,1,:axis&l=0&palette.0=reds&palette.1=blues
