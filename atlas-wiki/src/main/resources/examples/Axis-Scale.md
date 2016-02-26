> [[Home]] ▸ Examples

There are currently two scales that can be used for an axis:

* [Linear](#linear)
* [Logarithmic](#logarithmic)

## Linear

Linear is the default scale for an axis.

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,42

## Logarithmic

Add `o=1` to set axis to logarithmic. This is often used if two lines with significantly
different magnitudes are on the same axis. In many cases, using a separate [Y-axis](Multi-Y)
can be a better option that doesn't distort the line as much.

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,42&o=1

