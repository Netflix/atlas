> [[Home]] ▸ Examples

In addition to the [line style](Line-Styles) and [legend](Legends) the following attributes
can be adjusted:

* [Color](#color)
* [Transparency](#transparency)
* [Line Width](#line-width)

## Color

By default the color will come from the [palette](Color-Palettes) that is in use. However the
color for a line can also be set explicitly using the [:color](style-color) operator:

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,f00,:color&no_legend=1

Note, that for a group by all results will get the same attributes, so in this case all would
end up being the same color:

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by,f00,:color&no_legend=1

## Transparency

The transparency of a line can be set using the [:alpha](style-alpha) operator or by explicitly
setting the alpha channel as part of the [color](style-color).

/api/v1/graph?s=e-2d&e=2012-01-01T00:00&q=name,sps,:eq,:dup,6h,:offset,:area,40,:alpha&no_legend=1

Setting the alpha explicitly as part of the color:

/api/v1/graph?s=e-2d&e=2012-01-01T00:00&q=name,sps,:eq,:dup,6h,:offset,:area,40ff0000,:color&no_legend=1

## Line Width

Adjust the stroke width used for a line:

/api/v1/graph?s=e-1w&e=2012-01-01T00:00&q=name,sps,:eq,:dup,6h,:offset,3,:lw&no_legend=1

