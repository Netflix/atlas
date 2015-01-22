This page is a reference for the graph API provided by
Atlas. For a quick overview by example see the [[Examples]] page.

### URI

`/api/v1/graph?q=<expr>&[CLOUD:OPTIONS]`

## Query Parameters

### Callback (callback)

If the format is `json`, the callback is used for providing
[JSONP](http://en.wikipedia.org/wiki/JSONP) output. This parameter is
ignored for all other formats.

### Consolidation Function (cf)

Consolidation is used to convert between the step size used for storage
and the step size used in the graph, e.g., converting data stored with a
value each minute to data with a value every 5 minutes that will look
presentable in a graph.

<table>
  <tbody>
    <tr>
      <th>
        <p>Value</p>
      </th>
      <th>
        <p>Description</p>
      </th>
    </tr>
    <tr>
      <td>
        <p>avg</p>
      </td>
      <td>
        <p>Average of all the values in the interval.</p>
      </td>
    </tr>
    <tr>
      <td>
        <p>min</p>
      </td>
      <td>
        <p>Minimum value for the interval.</p>
      </td>
    </tr>
    <tr>
      <td>
        <p>max</p>
      </td>
      <td>
        <p>Maximum value for the interval.</p>
      </td>
    </tr>
    <tr>
      <td>
        <p>total</p>
      </td>
      <td>
        <p>Sum of all values in the interval.</p>
      </td>
    </tr>
  </tbody>
</table>

### End time (e)
Specifies the end time of the graph. Permitted values:

<table>
  <tbody>
    <tr>
      <th>
        <p>Value</p>
      </th>
      <th>
        <p>Description</p>
      </th>
    </tr>
    <tr>
      <td>
        <p>now</p>
      </td>
      <td>
        <p>Current time, this is the default.</p>
      </td>
    </tr>
    <tr>
      <td>
        <p>unix timestamp</p>
      </td>
      <td>
        <p>A number representing the number of seconds since the epoch, e.g. <code>date +%s</code>.</p>
      </td>
    </tr>
    <tr>
      <td>
        <p>ISO8601 timestamp</p>
      </td>
      <td>
        <p>A string following the ISO8601 date time format, e.g. <code>date +%Y-%m-%dT%H:%M</code>.</p>
      </td>
    </tr>
  </tbody>
</table>

### Format (format)

Specifies the output format to use. The default is `png`.

<table>
  <tbody>
    <tr>
      <th>
        <p>Value</p>
      </th>
      <th>
        <p>Description</p>
      </th>
    </tr>
    <tr>
      <td>
        <p>csv</p>
      </td>
      <td>
        <p>Comma-separated value file that can be imported into a spreadsheet.</p>
      </td>
    </tr>
    <tr>
      <td>
        <p>json</p>
      </td>
      <td>
        <p>Outputs the graph data as a JSON object.</p>
      </td>
    </tr>
    <tr>
      <td>
        <p>png</p>
      </td>
      <td>
        <p>Output will be a png image of the graph.</p>
      </td>
    </tr>
    <tr>
      <td>
        <p>txt</p>
      </td>
      <td>
        <p>Similar to CSV, but uses mime-type <code>text/plain</code> so it will render in the browser and uses tabs instead of commas to make it easier to read.</p>
      </td>
    </tr>
  </tbody>
</table>

### Height (h)

Integer representing the height of the chart area (see
[rrdgraph](http://oss.oetiker.ch/rrdtool/doc/rrdgraph.en.html#ISize)).
This is not the size of the final image. The final image size will be
determined after titles, legends, etc have been added in. If you want a
consistent image size, then disable legends.

### Hide Legend (no\_legend)

Turns off the legend that is normally shown at the bottom of the graph.
Acceptable values are 0 (false, the default) and 1 (true).

### Logarithmic Y-Axis Scale (o)

Configures the default Y-Axis scale to be logarithmic. Acceptable values
are 0 (false, the default) and 1 (true).

### Logarithmic Y-Axis Scale (o.n)

Configures the  scale of the Y-Axis specified by 'n' to be logarithmic,
where n is an integer in range 0 - 4.  Acceptable values are 0 (false,
the default) and 1 (true). 

For example, &0.1=1 sets the scale of axis 1 to be logarithmic

### Lower Limit (l)

Integer representing the default lower bound for all y-axis. If a lower
bound is not set, then one will be determined automatically based on the
data to be shown in the graph.

### Lower Limit (l.n)

Integer representing the lower bound for the particular y-axis specified
by 'n', where n is an integer in range 0 - 4.  If a lower bound is not
set, then one will be determined automatically based on the data to be
shown in the graph. 

For example, &l.1=100.0 sets the lower bound of axis 1

### Metric Query (q)

Query expression used to select a set of metrics and manipulate them for
presentation in a graph. The query expression can use db, query, graph,
and std commands described in the reference. For examples see the graph
examples page .

### Stack (stack)

Configures the default stack setting for all y-axis.  All lines in the
graph will be plotted as filled areas stacked on top of each other, per
y-axis. Acceptable values are 0 (false, the default) and 1 (true).

### Stack (stack.n)

Configures the  stack setting for the particular y-axis indicated by
'n', where n is an integer in range 0 - 4.  All lines in the graph which
are assigned to the specified y-axis will be plotted as filled areas
stacked on top of each other. 

Acceptable values are 0 (false, the default) and 1 (true).  For example,
&stack.1=1 turns the stacking behavior on for y-axis 1.

### Start time (s)

Specifies the start time of the graph. Permitted values:

<table>
  <tbody>
    <tr>
      <th>
        <p>Value</p>
      </th>
      <th>
        <p>Description</p>
      </th>
    </tr>
    <tr>
      <td>
        <p>relative time</p>
      </td>
      <td>
        <p>Time relative to the specified end time. The default is <code>e-3h</code>.</p>
      </td>
    </tr>
    <tr>
      <td>
        <p>unix timestamp</p>
      </td>
      <td>
        <p>A number representing the number of seconds since the epoch, e.g. <code>date +%s</code>.</p>
      </td>
    </tr>
    <tr>
      <td>
        <p>ISO8601 timestamp</p>
      </td>
      <td>
        <p>A string following the ISO8601 date time format, e.g. <code>date +%Y-%m-%dT%H:%M</code>.</p>
      </td>
    </tr>
  </tbody>
</table>

### Step size (step)

Integer representing the number of seconds separating each data point.
The default is calculated based on the width of the graph so that there
is at least one pixel for each point. A maximum of 10080 points (1 week
at minute resolution) can be returned. Use ISO 8601 syntax for
durations. For example, step=PT60S represents 60 seconds.

### Time Zone (tz)

String representing the timezone to use when displaying the data. The
default is `US/Pacific`.

### Title (title)

String used for the title of the graph. By default a graph will not have
a title.

### Upper Limit (u)

Integer representing the default upper bound for the y-axis. If an upper
bound is not set, then one will be determined automatically based on the
data to be shown in the graph.

### Upper Limit (u.n)

Integer representing the upper bound for the particular y-axis specified
by 'n', where n is an integer in range 0 - 4.  If an upper bound is not
set, then one will be determined automatically based on the data to be
shown in the graph.

For example, &u.1=200.0 sets the upper bound of axis 1

### Width (w)

Integer representing the width of the chart area (see
[rrdgraph](http://oss.oetiker.ch/rrdtool/doc/rrdgraph.en.html#ISize)).
This is not the size of the final image. The final image size will be
determined after titles, legends, etc have been added in. If you want a
consistent image size, then disable legends.

### Y-Axis Label (ylabel)

String used as a default label for the y-axis of the graph. By default a
graph will not have a y-axis label.

### Y-Axis Label (ylabel.n)

String used as a label for the particular y-axis of the graph specified
by 'n', where n is an integer in range 0 - 4. By default a graph will
not have a y-axis label on any y-axis.

For example, &ylabel.1=Some Metric sets the label of axis 1
