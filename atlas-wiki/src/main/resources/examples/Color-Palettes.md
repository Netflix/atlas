> [[Home]] ▸ Examples

The following color palettes are supported:

* [armytage](#armytage)
* [epic](#epic)
* [blues](#blues)
* [greens](#greens)
* [oranges](#oranges)
* [purples](#purples)
* [reds](#reds)
* [custom](#custom)

There is also a [hashed selection](#hashed-selection) mode that can be used so that a line
with a given label will always get the same color.

## Armytage

This is the default color palette, it comes from the paper
[A Colour Alphabet and the Limits of Colour Coding](http://www.aic-color.org/journal/previous_archivos/v5/jaic_v5_06.pdf)
by Paul Green-Armytage. Two colors, Xanthin and Yellow, are excluded because users found them hard
to distinguish from a white background when used for a single pixel line. So overall there are
24 distinct colors with this palette.

/api/v1/graph?q=1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1&stack=1&palette=armytage&no_legend=1&e=2012-01-01T09:00&tz=UTC

## Epic

This is a legacy palette that alternates between shades of red, green, and blue. It is supported
for backwards compatibility, but not recommended.

/api/v1/graph?q=1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1&stack=1&palette=epic&no_legend=1&e=2012-01-01T09:00&tz=UTC

## Blues

Shades of blue.

/api/v1/graph?q=1,1,1,1,1,1,1&stack=1&palette=blues&no_legend=1&e=2012-01-01T09:00&tz=UTC

## Greens

Shades of green.

/api/v1/graph?q=1,1,1,1,1,1,1&stack=1&palette=greens&no_legend=1&e=2012-01-01T09:00&tz=UTC

## Oranges

Shades of orange.

/api/v1/graph?q=1,1,1,1,1,1,1&stack=1&palette=oranges&no_legend=1&e=2012-01-01T09:00&tz=UTC

## Purples

Shades of purple.

/api/v1/graph?q=1,1,1,1,1,1,1&stack=1&palette=purples&no_legend=1&e=2012-01-01T09:00&tz=UTC

## Reds

Shades of red.

/api/v1/graph?q=1,1,1,1,1,1,1&stack=1&palette=reds&no_legend=1&e=2012-01-01T09:00&tz=UTC

## Custom

A custom color palette can be provided for a graph by using a prefix of `colors:` followed by
a comma separated list of [hex color](style-color) values. This is mainly used to customize the
colors for the result of a group by where you cannot set the color for each line using
[:color](style-color).

/api/v1/graph?q=1,1,1,1,1,1&stack=1&palette=colors:1a9850,91cf60,d9ef8b,fee08b,fc8d59,d73027&no_legend=1&e=2012-01-01T09:00&tz=UTC

## Hashed Selection

Any of the palettes above can be prefixed with `hash:` to select the color using a hashing
function on the label rather than picking the next color from the list. The primary advantage
is that the selected color will always be the same for a given label using a particular
palette. However, some nice properties of the default mode are lost:

* Colors can be duplicated even with a small number of lines. Hash collisions will result
  in the same color.
* The palettes are ordered to try and make the stacked appearance and legends easier to
  read. For the [armytage](#armytage) palette it is ordered so adjacent colors are easy
  to distinguish. For the palettes that are shades of a color they are ordered from dark
  to light shades to create a gradient effect. Hashing causes an arbitrary ordering of
  the colors from the palette.

The table below illustrates the difference by adding some additional lines to a chart
for the second row:

<table>
<thead>
  <th width="50%">armytage</th>
  <th width="50%">hash:armytage</th>
</thead>
<tbody>
<tr>
  <td><img src="/api/v1/graph?e=2012-01-01T00:00&s=e-2d&q=name,sps,:eq,(,nf.cluster,),:by&stack=1&palette=armytage&layout=iw&w=350&h=150&no_legend_stats=1"/></td>
  <td><img src="/api/v1/graph?e=2012-01-01T00:00&s=e-2d&q=name,sps,:eq,(,nf.cluster,),:by&stack=1&palette=hash:armytage&layout=iw&w=350&h=150&no_legend_stats=1"/></td>
</tr>
<tr>
  <td><img src="/api/v1/graph?e=2012-01-01T00:00&s=e-2d&q=40e3,20e3,name,sps,:eq,(,nf.cluster,),:by&stack=1&palette=armytage&layout=iw&w=350&h=150&no_legend_stats=1"/></td>
  <td><img src="/api/v1/graph?e=2012-01-01T00:00&s=e-2d&q=40e3,20e3,name,sps,:eq,(,nf.cluster,),:by&stack=1&palette=hash:armytage&layout=iw&w=350&h=150&no_legend_stats=1"/></td>
</tr>
</tbody>
</table>

Example:

/api/v1/graph?q=name,sps,:eq,(,nf.cluster,),:by,:stack&stack=1&palette=hash:armytage&no_legend=1&e=2012-01-01T09:00&tz=UTC
