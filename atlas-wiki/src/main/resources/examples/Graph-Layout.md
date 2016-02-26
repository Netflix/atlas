> [[Home]] ▸ Examples

The diagram below shows the parts of an Atlas graph and will be used when describing
the behavior for various options.

<img alt="Image Layout" width="300" height="348" src="http://netflix.github.io/atlas/images/wiki/image_layout.svg">

The layout for graph images is trying to accomplish two main goals:

#### Usable Canvas Size

Keep the canvas usable regardless of the number of lines, axes, etc that are competing for space. For example, the canvas area should not become too small due to the number of lines on the chart. 

<table>
<tr>
<td><img alt="Legend 8 cores" width="329" height="269" src="https://raw.githubusercontent.com/wiki/Netflix/atlas/images/sample-8-cores.png"/></td>
<td><img alt="Legend 32 cores" width="329" height="269" src="https://raw.githubusercontent.com/wiki/Netflix/atlas/images/sample-32-cores.png"/></td>
</tr>
</table>

#### Canvas Alignment

Make it easy to align the canvas portion of several graphs on an html page. This is important because it makes it easier to find visual correlations between multiple graphs on a dashboard.

In particular if arranged in a grid with the image in the top left of each cell, then the canvas should line up vertically for columns:

<img alt="Vertical Alignment" width="535" height="676" src="https://raw.githubusercontent.com/wiki/Netflix/atlas/images/layout_canvas_vertical.png"/>

And horizontally for rows:

<img alt="Horizontal Alignment" width="535" height="676" src="https://raw.githubusercontent.com/wiki/Netflix/atlas/images/layout_canvas_horizontal.png"/>

In the graph layout diagram at the top, this is why variable components such as multi axes, legend entries, and warnings are positioned on either the right side or the bottom of the canvas.

## Modes

There are four supported layout modes that can be used with the [layout query parameter](Graph#image-size):

* `canvas`: the width or height are for the canvas component within the chart. The actual image size will be calculated based on the number of entries in the legend, number of axes, etc. This is the default behavior.
* `image`: the width or height are for the final image not including the zoom parameter. To try and adhere to layout goals when using this mode everything below the X-axes will automatically be suppressed. Vertical alignment will still hold as long as all graphs use the same number of Y-axes. Horizontal alignment will still hold as long as all graphs use the same number of X-axes.
* `iw`: use exact image sizing for the width and canvas sizing for the height.
* `ih`: use exact image sizing for the height and canvas sizing for the width.

## Examples

### Canvas

/api/v1/graph?q=name,sps,:eq,:sum,(,nf.cluster,),:by&s=e-1d&e=2012-01-01T09:00&tz=UTC&layout=canvas&h=175&w=400

### Image

/api/v1/graph?q=name,sps,:eq,:sum,(,nf.cluster,),:by&s=e-1d&e=2012-01-01T09:00&tz=UTC&layout=image&h=175&w=400

### Image Width

/api/v1/graph?q=name,sps,:eq,:sum,(,nf.cluster,),:by&s=e-1d&e=2012-01-01T09:00&tz=UTC&layout=iw&h=175&w=400

### Image Height

/api/v1/graph?q=name,sps,:eq,:sum,(,nf.cluster,),:by&s=e-1d&e=2012-01-01T09:00&tz=UTC&layout=ih&h=175&w=400