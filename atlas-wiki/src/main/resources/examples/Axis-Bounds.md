
The upper and lower bounds for an axis can be set to an explicit floating point value or:

* `auto-style`: automatically determine the bounds based on the data and the style settings for
  that data. In particular, if the line style is [area](style-area) or [stack](style-stack), then
  the bounds will be adjusted to show the filled area. This is the default behavior.
* `auto-data`: automatically determine the bounds based on the data. This will only take into
  account the values of the lines. In the case of [stack](style-stack) it will account for the
  position of the stacked lines, but not the filled area.

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