> [[Home]] ▸ Examples

There are four [line styles](style-ls) available:

* [Line](#line)
* [Area](#area)
* [Stack](#stack)
* [Vertical Span](#vertical-span)

Multiple styles can be used in the same chart or combined with other operations.

* [Stacked Percentage](#stacked-percentage)
* [Combinations](#combinations)
* [Layering](#layering)

## Line

The default style is line.

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by,:line&no_legend=1

## Area

Area will fill the space between the line and 0 on the Y-axis. The [alpha](style-alpha) setting
is just used to help visualize the overlap.

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by,:area,40,:alpha&no_legend=1

Similarly for negative values:

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by,:neg,:area,40,:alpha&no_legend=1

## Stack

Stack is similar to [area](#area), but will stack the filled areas on top of each other.

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by,:stack&no_legend=1

Similarly for negative values:

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by,:neg,:stack&no_legend=1

## Stacked Percentage

The [stack](#stack) style can be combined with the [:pct](math-pct) operator to get a stacked
percentage chart for a group by:

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by,:pct,:stack&no_legend=1

## Vertical Span

The vertical span style converts non-zero to spans. This is often used to highlight some portion of
another line.

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,500e3,:gt,:vspan&no_legend=1

## Combinations

Line styles can be combined, e.g., to highlight the portion of a line that is above a
threshold:

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,:dup,500e3,:gt,:vspan,40,:alpha,500e3&no_legend=1

## Layering

The z-order is based on the order of the expression on the stack.

/api/v1/graph?q=t,name,sps,:eq,:sum,:set,t,:get,:stack,t,:get,1.1,:mul,6h,:offset,t,:get,4,:div,:stack&s=e-2d&e=2015-03-10T13:13&no_legend=1
