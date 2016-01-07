
Occasionally it is useful to show a graph, but the exact values need to be suppressed. This can
be useful for communicating with external support or including in a presentation. To avoid showing
the actual values disable tick labels using `no_tick_labels` and either
[disable the legend](Legends#disable) or [disable the legend stats](Legends#disable-stats).

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by&no_legend_stats=1&no_tick_labels=1