> [[Home]] ▸ Examples

Scales determine how the data value for a line will get mapped to the Y-Axis.
There are currently four scales that can be used for an axis:

<table>
<tr>
<td>Linear</td>
<td>Logarithmic</td>
<td>Power of 2</td>
<td>Square Root</td>
</tr>
<tr>
<td><a href="#linear"><img src="/api/v1/graph?e=2012-01-01T00:00&q=minuteOfHour,:time,1e3,:add,minuteOfHour,:time&scale=linear&layout=image&zoom=0.20"/></a></td>
<td><a href="#logarithmic"><img src="/api/v1/graph?e=2012-01-01T00:00&q=minuteOfHour,:time,1e3,:add,minuteOfHour,:time&scale=log&layout=image&zoom=0.20"/></a></td>
<td><a href="#power-of-2"><img src="/api/v1/graph?e=2012-01-01T00:00&q=minuteOfHour,:time,1e3,:add,minuteOfHour,:time&scale=pow2&layout=image&zoom=0.20"/></a></td>
<td><a href="#square-root"><img src="/api/v1/graph?e=2012-01-01T00:00&q=minuteOfHour,:time,1e3,:add,minuteOfHour,:time&scale=sqrt&layout=image&zoom=0.20"/></a></td>
</tr>
</table>

## Linear

A linear scale uniformly maps the input values (domain) to the Y-axis location (range).
If _v_ is datapoint in a time series, then _y=m*v+b_ where _m_ and _b_ are automatically
chosen based on the domain and range.

This is the default scale for an axis and will get used if no explicit scale is set. Since
1.6, it can also be used explicitly:

/api/v1/graph?e=2012-01-01T00:00&q=minuteOfHour,:time,1e3,:add,minuteOfHour,:time&scale=linear

## Logarithmic

A logarithmic scale emphasizes smaller values when mapping the input values (domain) to the
Y-axis location (range). This is often used if two lines with significantly different magnitudes
are on the same axis. If _v_ is datapoint in a time series, then _y=m*log(v)+b_ where _m_
and _b_ are automatically chosen based on the domain and range. In many cases, using a separate
[Y-axis](Multi-Y) can be a better option that doesn't distort the line as much.

To use this mode, add `scale=log` (prior to 1.6 use `o=1`). 

/api/v1/graph?e=2012-01-01T00:00&q=minuteOfHour,:time,1e3,:add,minuteOfHour,:time&scale=log

## Power of 2

Since 1.6.

A power scale that emphasizes larger values when mapping the input values (domain) to the
Y-axis location (range). If _v_ is datapoint in a time series, then _y=m*v<sup>2</sup>+b_
where _m_ and _b_ are automatically chosen based on the domain and range. To emphasize smaller
values see the [square root scale](#square-root).

To use this mode, add `scale=pow2`.

/api/v1/graph?e=2012-01-01T00:00&q=minuteOfHour,:time,1e3,:add,minuteOfHour,:time&scale=pow2

## Square Root

Since 1.6.

A power scale that emphasizes smaller values when mapping the input values (domain) to the
Y-axis location (range). If _v_ is datapoint in a time series, then _y=m*v<sup>0.5</sup>+b_
where _m_ and _b_ are automatically chosen based on the domain and range. To emphasize larger
values see the [power of 2 scale](#power-of-2).

To use this mode, add `scale=sqrt`.

/api/v1/graph?e=2012-01-01T00:00&q=minuteOfHour,:time,1e3,:add,minuteOfHour,:time&scale=sqrt


