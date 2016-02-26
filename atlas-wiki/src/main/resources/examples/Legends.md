> [[Home]] ▸ Examples

Options for adjusting legend:

* [Automatic](#automatic)
* [Explicit](#explicit)
* [Variables](#variables)
* [Disable](#disable)
* [Disable Stats](#disable-stats)

## Automatic

If no [explicit](#explicit) legend is specified, then the system will generate an automatic
legend that summarizes the expression. There is no particular guarantee about what it will contain
and in some cases it is difficult to generate a usable legend automatically. Example:

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=hourOfDay,:time,100,:mul,minuteOfHour,:time,:add

## Explicit

The legend for a line can be explicitly set using the [:legend](style-legend) operator.

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=hourOfDay,:time,100,:mul,minuteOfHour,:time,:add,time+value,:legend

## Variables

Tag keys can be used as variables to plug values into the legend. This is useful when working
with group by operations to customize the legend for each output. The variable can be expressed
as a `$` followed by the tag key if it is the only part of the legend:

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by,$nf.cluster,:legend

Or as `$(` the tag key and a closing `)` if combined with other text:

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by,cluster+$(nf.cluster)+sps,:legend

## Disable

Legends can be disabled using the `no_legend` graph parameter.

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by&no_legend=1

## Disable Stats

You can also save veritical space and keep the legend by disabling the summary stats shown in the
legend using the `no_legend_stats` graph parameter.

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by&no_legend_stats=1
