
/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:sum

## Time Zones

/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:sum,name,sps,:eq,nf.cluster,nccp-xbox,:eq,:and,:sum,:area
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:sum,name,sps,:eq,nf.cluster,nccp-xbox,:eq,:and,:sum,:area&tz=UTC

## Customizing Line Colors and Legend Text

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:sum,000000,:color,silverlight,:legend,name,sps,:eq,nf.cluster,nccp-xbox,:eq,:and,:sum,ccccff,:color,xbox,:legend

This adds an `$(atlas.offset)` variable for use in showing the offset in the legends:

/api/v1/graph?e=2012-01-01T00:00&q=(,0h,1d,1w,),(,name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:sum,:swap,:offset,$(name)+(offset%3D$(atlas.offset)),:legend,),:each

## Group By and Stack

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-.*,:re,:and,:sum,(,nf.cluster,),:by,$nf.cluster,:legend&stack=1

## Percentages

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-.*,:re,:and,:sum,(,nf.cluster,),:by,:pct,$nf.cluster,:legend&stack=1

## Upper and Lower Bounds

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-.*,:re,:and,:sum,(,nf.cluster,),:by,$nf.cluster,:legend&stack=1&l=0&u=500000

## Basic Math and Logarithmic Axis

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:dup,:sum,:swap,:count,:over,:over,:div,average,:legend,:rot,sum,:legend,:rot,count,:legend&o=1
/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:dup,:sum,:swap,:count,:over,:over,:div,average,:legend,:rot,sum,:legend,:rot,count,:legend&scale=log
/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:dup,:sum,:swap,:count,:over,:over,:div,average,:legend,:rot,sum,:legend,:rot,count,:legend&scale=pow2
/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:dup,:sum,:swap,:count,:over,:over,:div,average,:legend,:rot,sum,:legend,:rot,count,:legend&scale=sqrt
/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:dup,:sum,:swap,:count,:over,:over,:div,average,:legend,:rot,sum,:legend,:rot,count,:legend&scale=linear
/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:dup,:sum,:swap,:count,:over,:over,:div,average,:legend,:rot,sum,:legend,:rot,count,:legend&scale=linear&o=1

/api/v1/graph?e=2012-01-01T00:00&s=e-2d&q=name,sps,:eq,:sum,:dup,:dup,min,:stat,:sub,:swap,max,:stat,0.5,:mul,:sub&scale=linear
/api/v1/graph?e=2012-01-01T00:00&s=e-2d&q=name,sps,:eq,:sum,:dup,:dup,min,:stat,:sub,:swap,max,:stat,0.5,:mul,:sub&scale=log
/api/v1/graph?e=2012-01-01T00:00&s=e-2d&q=name,sps,:eq,:sum,:dup,:dup,min,:stat,:sub,:swap,max,:stat,0.5,:mul,:sub&scale=pow2
/api/v1/graph?e=2012-01-01T00:00&s=e-2d&q=name,sps,:eq,:sum,:dup,:dup,min,:stat,:sub,:swap,max,:stat,0.5,:mul,:sub&scale=sqrt

## Average

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:avg,avg+sps+for+silverlight,:legend

## Titles and Legends

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:avg,avg+sps+for+silverlight,:legend&no_legend=1&title=Silverlight+SPS&ylabel=Starts+per+second

## Line on Area

Area must be drawn first or line will be covered up.

/api/v1/graph?e=2012-01-01T00:00&q=nf.cluster,nccp-silverlight,:eq,name,sps,:eq,:and,:sum,:dup,10000,:add,:area,:swap

## Stacked Areas plus a Line

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:sum,:area,name,sps,:eq,nf.cluster,nccp-ps3,:eq,:and,:sum,:stack,name,sps,:eq,:avg,1000,:mul

## Transparency

/api/v1/graph?e=2012-01-01T00:00&q=nf.cluster,nccp-silverlight,:eq,name,sps,:eq,:and,:sum,:dup,10000,:add,:area,400000ff,:color

/api/v1/graph?e=2012-01-01T00:00&q=nf.cluster,nccp-silverlight,:eq,name,sps,:eq,:and,:sum,:dup,10000,:add,:area,40,:alpha

## Double Exponential Smoothing

Delta between actual and predicted shown as area:

/api/v1/graph?tz=UTC&e=2012-01-01T12:00&s=e-12h&w=750&h=150&l=0&q=nf.cluster,alerttest,:eq,name,requestsPerSecond,:eq,:and,:sum,:dup,:des-simple,0.9,:mul,:2over,:sub,:abs,:area,40,:alpha,:rot,$name,:legend,:rot,prediction,:legend,:rot,delta,:legend

Vertical spans showing outliers:

/api/v1/graph?tz=UTC&e=2012-01-01T12:00&s=e-12h&w=750&h=150&l=0&q=nf.cluster,alerttest,:eq,name,requestsPerSecond,:eq,:and,:sum,:dup,:des-simple,0.9,:mul,:2over,:lt,:rot,$name,:legend,:rot,prediction,:legend,:rot,:vspan,60,:alpha,alert+triggered,:legend

## Math with Time Shifts

Basic example, subtract value from 1 week ago with current value:

/api/v1/graph?e=2012-01-01T12:00&s=e-12h&tz=UTC&q=nf.cluster,alerttest,:eq,name,requestsPerSecond,:eq,:and,:sum,:dup,1w,:offset,:sub,:area,delta+week+over+week,:legend&h=150&w=750

Compute average for the previous 3 weeks and show percentage of the change:

/api/v1/graph?e=2012-01-01T12:00&s=e-12h&tz=UTC&q=nf.cluster,alerttest,:eq,name,requestsPerSecond,:eq,:and,:sum,:dup,1w,:offset,:over,2w,:offset,:add,:over,3w,:offset,:add,3,:div,:2over,:swap,:over,:sub,:abs,:swap,:div,100,:mul,:rot,requestsPerSecond,:legend,:rot,average+for+previous+3+weeks,:legend,:rot,:area,40,:alpha,percent+delta,:legend&h=150&w=750

## Smoothing a Line

Suppose you have a line that is quite rough, for example:

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,type,high-noise,:eq,:and,:sum&w=750&h=100&no_legend=1&s=e-12h

One option is to apply a smoothing function, currently we support DES and trend which computes a rolling average for a given time window. DES example:

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,type,high-noise,:eq,:and,:sum,10,0.145,0.01,:des&w=750&h=100&no_legend=1&s=e-12h

Trend example:

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-ps3,:eq,:and,:avg,:dup,:dup,:dup,5m,:trend,100,:add,5m+trend,:legend,:rot,10m,:trend,200,:add,10m+trend,:legend,:rot,20m,:trend,300,:add,20m+trend,:legend,:rot,original+line,:legend,:-rot&w=750&h=300&s=e-12h

You can also smooth out the line by applying a larger step size, note this applies to all data on a given graph. For example, the same line at a 5 minute step:

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,type,high-noise,:eq,:and,:sum&step=PT5M&w=750&h=100&no_legend=1&s=e-12h

At a 20 minute step:

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,type,high-noise,:eq,:and,:sum&step=PT20M&w=750&h=100&no_legend=1&s=e-12h

## Multiple Y Axis

/api/v1/graph?e=2012-01-01T00:00&q=nf.node,alert1,:eq,:sum,nf.node,alert1,:eq,:count,1,:axis&ylabel.0=Axis%200&ylabel.1=Axis%201
