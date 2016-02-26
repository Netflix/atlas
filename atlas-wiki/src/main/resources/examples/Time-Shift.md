> [[Home]] ▸ Examples

A common use-case is to compare a given line with a shifted line to compare week-over-week or day-over-day. 

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:sum,:dup,1w,:offset

The `$(atlas.offset)` variable can be used to show the offset in a custom legend:

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:sum,:dup,1w,:offset,:list,(,$nf.cluster+(offset%3D$atlas.offset),:legend,),:each

