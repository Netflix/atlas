/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,),:offset,f00,:color
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,1d,),:offset,f00,:color
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,2,:lw,(,0h,1d,1w,),:offset,f00,:color
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,2,:lw,(,0h,1d,1w,),:offset,:stack

/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=42,name,sps,:eq,:sum,(,nf.cluster,),:by,1,:axis
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=42,name,sps,:eq,:sum,(,nf.cluster,),:by,1,:axis,37,:area,40,:alpha,2,:axis&u.2=100&l.2=0
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=42,name,sps,:eq,:sum,(,nf.cluster,),:by,1,:axis,37,:area,40,:alpha,2,:axis&u.2=100&l.2=0&stack=1
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=42,name,sps,:eq,:sum,(,nf.cluster,),:by,1,:axis,37,:area,40,:alpha,2,:axis&u.2=100&l.2=0&stack.1=1
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=42,name,sps,:eq,:sum,(,nf.cluster,),:by,1,:axis,f00,:color,37,:area,40,:alpha,2,:axis&u.2=100&l.2=0
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=42,name,sps,:eq,:sum,(,nf.cluster,),:by,1w,:offset,1,:axis,37,:area,40,:alpha,2,:axis&u.2=100&l.2=0
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=42,name,sps,:eq,:sum,:dup,1w,:offset,1,:axis,:swap,1,:axis,37,:area,40,:alpha,2,:axis&u.2=100&l.2=0
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=42,name,sps,:eq,:sum,:dup,1w,:offset,1,:axis,f00,:color,:swap,1,:axis,37,:area,40,:alpha,2,:axis&u.2=100&l.2=0

/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,2,:lw,(,0h,1d,1w,),:offset,:stack&zoom=2.0

/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=42,:area
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=42,:area&l=30
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=-42,:area
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=-42,:area&u=-30

/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=42,:area,40,:alpha,f00,:color
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=42,:area,40,:alpha,ffff0000,:color

/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=NaN
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=NaN,:area
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=NaN,:stack,1,:stack

/api/v1/graph?e=2015-05-26T19:52&q=minuteOfHour,:time&s=e-10m


# Layout
/api/v1/graph?q=name,sps,:eq,:sum,(,nf.cluster,),:by,:stack&s=e-3h&e=2015-03-10T13:13&w=70&h=70&layout=canvas
/api/v1/graph?q=name,sps,:eq,:sum,(,nf.cluster,),:by,:stack&s=e-3h&e=2015-03-10T13:13&w=70&h=70&layout=image
/api/v1/graph?q=name,sps,:eq,:sum,(,nf.cluster,),:by,:stack&s=e-3h&e=2015-03-10T13:13&w=70&h=70&layout=iw
/api/v1/graph?q=name,sps,:eq,:sum,(,nf.cluster,),:by,:stack&s=e-3h&e=2015-03-10T13:13&w=100&h=70&layout=iw
/api/v1/graph?q=name,sps,:eq,:sum,(,nf.cluster,),:by,:stack&s=e-3h&e=2015-03-10T13:13&w=160&h=70&layout=iw
/api/v1/graph?q=name,sps,:eq,:sum,(,nf.cluster,),:by,:stack&s=e-3h&e=2015-03-10T13:13&w=270&h=70&layout=iw
/api/v1/graph?q=name,sps,:eq,:sum,(,nf.cluster,),:by,:stack&s=e-3h&e=2015-03-10T13:13&w=70&h=70&layout=ih
/api/v1/graph?q=name,sps,:eq,:sum,(,nf.cluster,),:by,:stack&s=e-3h&e=2015-03-10T13:13&w=70&h=7500&layout=iw&title=foo+bar+baz+dlfdkfdsfd

# Axis per line
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by&axis_per_line=1
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:sum,(,nf.node,),:by&axis_per_line=1
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,42&axis_per_line=1
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,42,1,:axis&axis_per_line=1
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,42&axis_per_line=1&ylabel.0=zero&ylabel.1=one
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,42,1,:axis&ylabel.0=zero&ylabel.1=one
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,42,1,:axis&ylabel=default&ylabel.0=zero&ylabel.1=one

# Turning off tick labels
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by&tick_labels=off
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=42,name,sps,:eq,:sum,(,nf.cluster,),:by,1,:axis&tick_labels.1=off
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-wii,:lt,:and,:sum,(,nf.cluster,),:by&tick_labels=off&axis_per_line=1
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-wii,:lt,:and,:sum,(,nf.cluster,),:by&tick_labels.2=off&axis_per_line=1
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-wii,:lt,:and,:sum,(,nf.cluster,),:by&tick_labels.2=off&axis_per_line=1&no_legend_stats=1

# Palettes
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by,:pct,$nf.cluster,:legend,:stack&palette=armytage
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by,:pct,$nf.cluster,:legend,:stack&palette=epic
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by,:pct,$nf.cluster,:legend,:stack&palette=bw
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by,:pct,$nf.cluster,:legend,:stack&palette=colors:1a9850,91cf60,d9ef8b,fee08b,fc8d59,d73027
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by,:pct,$nf.cluster,:legend,:stack&palette=blues
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by,:pct,$nf.cluster,:legend,:stack&palette=greens
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by,:pct,$nf.cluster,:legend,:stack&palette=oranges
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by,:pct,$nf.cluster,:legend,:stack&palette=purples
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by,:pct,$nf.cluster,:legend,:stack&palette=reds

# Hashed Palettes
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=40,20,name,sps,:eq,:sum,(,nf.cluster,),:by,:pct,$nf.cluster,:legend,:stack&palette=hash:armytage

# Axis palette
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by,name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:sum,(,nf.node,),:by,1,:axis,1,2,:axis&palette=epic&palette.1=oranges

# Auto bounds
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=200,:stack,100,:stack&l=auto-data
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=200,:stack,100,:stack&l=auto-style
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=200,:stack,100,:stack&l=47
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=-200,:stack,-100,:stack&u=auto-data
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=-200,:stack,-100,:stack&u=auto-style
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=-200,:stack,-100,:stack&u=-47

# Binary prefixes for y-labels
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by&tick_labels=binary
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by,1e3,:add&tick_labels=binary
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by,1e18,:add&tick_labels=binary
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-wii,:lt,:and,:sum,(,nf.cluster,),:by,1e9,:add&tick_labels=binary&axis_per_line=1
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-wii,:lt,:and,:sum,(,nf.cluster,),:by,1e9,:add&tick_labels.2=binary&axis_per_line=1

# issue-349, legend sorting
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,asc,:order
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,desc,:order
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,max,:sort
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,max,:sort,desc,:order
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,min,:sort
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,avg,:sort
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,total,:sort
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,last,:sort
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,count,:sort
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,legend,:sort
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,foo,:sort
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by&sort=foo&order=asc
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by&sort=foo&order=desc
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by&sort=max&order=desc
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by&sort=max
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by&order=desc
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,max,:sort,desc,:order&sort=max

# Limits
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,2,:limit
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,asc,:order,2,:limit
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,desc,:order,2,:limit
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,3,:head
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,1,:head
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,0,:head
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=12e3,threshold,:legend,name,sps,:eq,:sum,(,nf.cluster,),:by,-1,:head

# sorting with NaN values
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9&sort=max&order=desc
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9,1,2,NaN,3,NaN,4,5,6,7,8,NaN,8,8,NaN,8,9&sort=max&order=asc
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=1,NaN,2,NaN,-1&sort=max&order=asc
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=1,NaN,2,NaN,-1&sort=max&order=desc


# filtering
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by,:stat-max,30e3,:gt,:filter
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by,:stat-max,15e3,:gt,:filter
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by,:stat-max,15e3,:gt,:filter,42
/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,(,nf.cluster,),:by,:stat-max,15e3,:gt,:filter,:dup

# Stack crossing axis
/api/v1/graph?s=e-2d&e=2012-01-01T00:00&q=-6e3,:stack,10e3,:stack,name,sps,:eq,30e3,:sub,:stack,-5e3,:stack&tz=UTC&layout=image&w=600&h=400&no_legend=1

# Multi-Y and scales
/api/v1/graph?s=e-2d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,20e3,:sub,:stack,(,1,2,3,4,),(,name,sps,:eq,:sum,:swap,:axis,),:each&scale.1=log&scale.2=pow2&scale.3=sqrt&scale.4=linear
/api/v1/graph?s=e-2d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,20e3,:sub,:stack,(,1,2,3,4,),(,name,sps,:eq,:sum,:swap,:axis,),:each&scale.0=sqrt&scale.2=pow2&scale.3=log&scale.4=linear

# Long time ranges, NoSuchElementException bug
/api/v1/graph?q=42&s=e-12month&e=2017-06-27T00:00&tz=UTC&w=430&h=120&layout=iw
/api/v1/graph?q=42&s=e-3y&e=2017-06-27T00:00&tz=UTC&w=430&h=120&layout=iw
/api/v1/graph?q=42&s=e-30y&e=2017-06-27T00:00&tz=UTC&w=430&h=120&layout=iw

# Issue 684, errors from db actor
/api/v1/graph?s=e-2d&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.node,),:by

# Issue 565, time span operator
/api/v1/graph?s=e-2d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,e-6h,ge,:time-span,:vspan,40,:alpha
