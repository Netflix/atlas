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

## Armytage

This is the default color palette, it comes from the paper
[A Colour Alphabet and the Limits of Colour Coding](http://aic-colour-journal.org/index.php/JAIC/article/viewFile/19/16)
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
