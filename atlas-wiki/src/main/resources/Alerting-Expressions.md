The stack language provides some basic techniques to convert an input line into a set of signals
that can be used to trigger and visualize alert conditions. This section assumes a familiarity
with the stack language and the [alerting philosophy](Alerting-Philosophy)

## Threshold Alerts

To start we need an input metric. For this example the input will be a sample metric showing
high CPU usage for a period:

/api/v1/graph?s=e-3h&e=2012-01-01T07:00&tz=UTC&l=0&h=100&q=nf.app,alerttest,:eq,name,ssCpuUser,:eq,:and,:sum

Lets say we want to trigger an alert when the CPU usage goes above 80%. To do that simply use the
[:gt](math-gt) operator and append `80,:gt` to the query:

/api/v1/graph?s=e-3h&e=2012-01-01T07:00&tz=UTC&l=0&h=100&q=nf.app,alerttest,:eq,name,ssCpuUser,:eq,:and,:sum,80,:gt

The result is a _signal line_ that is non-zero, typically 1, when in a triggering state and zero
when everything is fine.

## Dampening

Our threshold alert above will trigger if the CPU usage is ever recorded to be above the
threshold. Alert conditions are often combined with a check for the number of occurences. This
is done by using the [:rolling-count](stateful-rolling-count) operator to get a line showing
how many times the input signal has been true withing a specified window and then applying a
second threshold to the rolling count.

<table>
<tr>
<td>Input</td>
<td>Rolling count</td>
<td>Dampened signal</td>
</tr>
<tr>
<td><img src="/api/v1/graph?s=e-3h&e=2012-01-01T07:00&tz=UTC&l=0&h=100&w=210&layout=image&q=nf.app,alerttest,:eq,name,ssCpuUser,:eq,:and,:sum,80,:gt" /></td>
<td><img src="/api/v1/graph?s=e-3h&e=2012-01-01T07:00&tz=UTC&l=0&h=100&w=210&layout=image&q=nf.app,alerttest,:eq,name,ssCpuUser,:eq,:and,:sum,80,:gt,5,:rolling-count" /></td>
<td><img src="/api/v1/graph?s=e-3h&e=2012-01-01T07:00&tz=UTC&l=0&h=100&w=210&layout=image&q=nf.app,alerttest,:eq,name,ssCpuUser,:eq,:and,:sum,80,:gt,5,:rolling-count,4,:gt" /></td>
</tr>
<tr>
<td><pre>
nf.app,alerttest,<a href="https://github.com/Netflix/atlas/wiki/query-eq">:eq</a>,
name,ssCpuUser,<a href="https://github.com/Netflix/atlas/wiki/query-eq">:eq</a>,
<a href="https://github.com/Netflix/atlas/wiki/query-and">:and</a>,
<a href="https://github.com/Netflix/atlas/wiki/data-sum">:sum</a>,
80,<a href="https://github.com/Netflix/atlas/wiki/math-gt">:gt</a>
</pre></td>
<td><pre>
nf.app,alerttest,<a href="https://github.com/Netflix/atlas/wiki/query-eq">:eq</a>,
name,ssCpuUser,<a href="https://github.com/Netflix/atlas/wiki/query-eq">:eq</a>,
<a href="https://github.com/Netflix/atlas/wiki/query-and">:and</a>,
<a href="https://github.com/Netflix/atlas/wiki/data-sum">:sum</a>,
80,<a href="https://github.com/Netflix/atlas/wiki/math-gt">:gt</a>,
<b>5,<a href="https://github.com/Netflix/atlas/wiki/stateful-rolling‐count">:rolling-count</a></b>
</pre></td>
<td><pre>
nf.app,alerttest,<a href="https://github.com/Netflix/atlas/wiki/query-eq">:eq</a>,
name,ssCpuUser,<a href="https://github.com/Netflix/atlas/wiki/query-eq">:eq</a>,
<a href="https://github.com/Netflix/atlas/wiki/query-and">:and</a>,
<a href="https://github.com/Netflix/atlas/wiki/data-sum">:sum</a>,
80,<a href="https://github.com/Netflix/atlas/wiki/math-gt">:gt</a>,
5,<a href="https://github.com/Netflix/atlas/wiki/stateful-rolling‐count">:rolling-count</a>,
<b>4,<a href="https://github.com/Netflix/atlas/wiki/math-gt">:gt</a></b>
</pre></td>
</tr>
</table>

## Visualization

A signal line is useful to tell whether or not something is in a triggered state, but can
be difficult for a person to follow. Alert expressions can be visualized by showing the
input, threshold, and triggering state on the same graph.

/api/v1/graph?s=e-3h&e=2012-01-01T07:00&tz=UTC&l=0&h=100&q=nf.app,alerttest,:eq,name,ssCpuUser,:eq,:and,:sum,80,:2over,:gt,:vspan,40,:alpha,triggered,:legend,:rot,input,:legend,:rot,threshold,:legend,:rot

## Summary

You should now know the basics of crafting an alert expression using the stack language. Other
topics that may be of interest:

* [Alerting Philosophy](Alerting-Philosophy): overview of best practices associated with alerts.
* [Stack Language Reference](Stack-Language-Reference): comprehensive list of avialable operators.
* [DES](DES): double exponential smoothing. A technique for detecting anomalies in normally clean
  input signals where a precise threshold is unknown. For example, the requests per second hitting
  a service.

