> [[Home]] ▸ Examples

Occasionally it is useful to show a graph, but the exact values need to be suppressed. This can
be useful for communicating with external support or including in a presentation. To avoid showing
the actual values disable tick labels using `tick_labels=off` and either
[disable the legend](Legends#disable) or [disable the legend stats](Legends#disable-stats).

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by&no_legend_stats=1&tick_labels=off

If you also want to suppress the time axis, then use the `only_graph` option:

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by&only_graph=1
