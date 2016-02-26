> [[Home]] ▸ Examples

The upper and lower bounds for an axis can be set to an explicit floating point value or:

* `auto-style`: automatically determine the bounds based on the data and the style settings for
  that data. In particular, if the line style is [area](style-area) or [stack](style-stack), then
  the bounds will be adjusted to show the filled area. This is the default behavior.
* `auto-data`: automatically determine the bounds based on the data. This will only take into
  account the values of the lines. In the case of [stack](style-stack) it will account for the
  position of the stacked lines, but not the filled area.

When selecting bounds it is important to think about how it can impact the perception of what
is shown. Automatic bounds can be useful for zooming in on the data, but can also lead to
[mis-perceptions](https://en.wikipedia.org/wiki/Misleading_graph#Axis_changes) for someone quickly
scanning a dashboard. Consider these two graphs showing percent CPU usage on an instance:

<table>
<tr>
<th>Automatic bounds</th>
<th>Explicit bounds</th>
</tr>
<tr>
<td><img alt="Automatic bounds" width="350" height="150" src="https://raw.githubusercontent.com/wiki/Netflix/atlas/images/axis-bounds-auto.png"/></td>
<td><img alt="Explicit bounds" width="350" height="150" src="https://raw.githubusercontent.com/wiki/Netflix/atlas/images/axis-bounds-explicit.png"/></td>
</tr>
</table>

The automatic bounds allows us to see much more detail, but could lead a casual observer to think
there were frequent large spikes in CPU usage rather than just noise on a machine with very little
load.

## Default Lower
  
/api/v1/graph?q=name,sps,:eq,nf.cluster,(,nccp-xbox,nccp-silverlight,),:in,:and,:sum,(,nf.cluster,),:by&s=e-1d&e=2012-01-01T09:00&tz=UTC

## Default Lower Stack
  
/api/v1/graph?q=name,sps,:eq,nf.cluster,(,nccp-xbox,nccp-silverlight,),:in,:and,:sum,(,nf.cluster,),:by,:stack&s=e-1d&e=2012-01-01T09:00&tz=UTC

## Default Upper
  
/api/v1/graph?q=name,sps,:eq,nf.cluster,(,nccp-xbox,nccp-silverlight,),:in,:and,:sum,(,nf.cluster,),:by,:neg&s=e-1d&e=2012-01-01T09:00&tz=UTC

## Default Upper Stack
  
/api/v1/graph?q=name,sps,:eq,nf.cluster,(,nccp-xbox,nccp-silverlight,),:in,:and,:sum,(,nf.cluster,),:by,:neg,:stack&s=e-1d&e=2012-01-01T09:00&tz=UTC
  
## Explicit Bounds

/api/v1/graph?q=name,sps,:eq,nf.cluster,(,nccp-xbox,nccp-silverlight,),:in,:and,:sum,(,nf.cluster,),:by,:stack&s=e-1d&e=2012-01-01T09:00&tz=UTC&l=100e3&u=200e3

## Auto Lower

/api/v1/graph?q=name,sps,:eq,nf.cluster,(,nccp-xbox,nccp-silverlight,),:in,:and,:sum,(,nf.cluster,),:by,:stack&s=e-1d&e=2012-01-01T09:00&tz=UTC&l=auto-data

## Auto Upper

/api/v1/graph?q=name,sps,:eq,nf.cluster,(,nccp-xbox,nccp-silverlight,),:in,:and,:sum,(,nf.cluster,),:by,:neg,:stack&s=e-1d&e=2012-01-01T09:00&tz=UTC&u=auto-data