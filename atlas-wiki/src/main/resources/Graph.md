This page is a reference for the graph API provided by
Atlas. For a quick overview by example see the [examples](Examples) page.

### URI

`/api/v1/graph?q=<expr>&[OPTIONS]`

## Query Parameters

### Data

The main query param is `q` which is the [query expression](Stack-Language) used by the user to select data. All query params related to fetching data:

| Name   | Description           | Default                    | Type                                 |
|--------|-----------------------|----------------------------|--------------------------------------|
| `q`    | Query expression      | must be specified by user  | [expr](Stack-Language)               |
| `step` | Step size for data    | auto                       | [duration](Time-Parameters#durations) |

> :warning: In most cases users should not set `step` directly. The `step` parameter
> is deprecated.

### Time

There are three parameters to control the time range used for a graph:

| Name   | Description | Default                    | Type |
|--------|-------------|----------------------------|------|
| `s`    | Start time  | `e-3h`[*](#defaults)       | [Time](Time-Parameters#time)              |
| `e`    | End time    | `now`[*](#defaults)        | [Time](Time-Parameters#time)              |
| `tz`   | Time zone   | `US/Pacific`[*](#defaults) | [Time zone ID](Time-Parameters#time-zone) |

For more information on the behavior see the [time parameters](Time-Parameters) page.

### Image Flags

| Name              | Description                              | Default      | Type                        |
|-------------------|------------------------------------------|--------------|-----------------------------|
| `title`           | Set the graph title                      | no title     | String                      |
| `no_legend`       | Suppresses the legend                    | `0`          | [boolean](#boolean-flags)   |
| `no_legend_stats` | Suppresses summary stats for the legend  | `0`          | [boolean](#boolean-flags)   |
| `axis_per_line`   | Put each line on a separate Y-axis       | `0`          | [boolean](#boolean-flags)   |
| `only_graph`      | Only show the graph canvas               | `0`          | [boolean](#boolean-flags)   |
| `vision`          | Simulate different vision types          | `normal`     | [vision type](Vision) |

### Image Size

There are four parameters to control the image size and layout used for a graph:

| Name     | Description                                    | Default             | Type                              |
|----------|------------------------------------------------|---------------------|-----------------------------------|
| `layout` | Mode for controlling exact or relative sizing  | `canvas`            | [layout mode](Graph-Layout#modes) |
| `w`      | Width of the canvas or image                   | `700`[*](#defaults) | int                               |
| `h`      | Height of the canvas or image                  | `300`[*](#defaults) | int                               |
| `zoom`   | Transform the size by a zoom factor            | `1.0`               | float                             |

For more information on the behavior see the [graph layout](Graph-Layout) page.

### Text Flags

| Name            | Description                                  | Default   | Type                                  |
|-----------------|----------------------------------------------|-----------|---------------------------------------|
| `number_format` | How to format numbers for text output types  | `%f`     | [float format](https://docs.oracle.com/javase/8/docs/api/java/util/Formatter.html#dndec) |

### Y-Axis

| Name             | Description                          | Default      | Type                           |
|------------------|--------------------------------------|--------------|--------------------------------|
| `stack`          | Set the default line style to stack  | `0`          | [boolean](#boolean-flags)      |
| `l`              | Lower bound for the axis             | `auto-style` | [axis bound](Axis-Bounds)      |
| `u`              | Upper bound for the axis             | `auto-style` | [axis bound](Axis-Bounds)      |
| `ylabel`         | Label for the axis                   | no label     | String                         |
| `palette`        | Color palette to use                 | `armytage`   | [palette](Color-Palettes)      |
| `o`              | Use a logarithmic scale              | `0`          | [boolean](#boolean-flags)      |
| `tick_labels`    | Set the mode to use for tick labels  | `decimal`    | [tick label mode](Tick-Labels) |

### Output Format

| Name        | Description                            | Default   | Type                                  |
|-------------|----------------------------------------|-----------|---------------------------------------|
| `format`    | Output format to use                   | `png`     | [output format](Output-Formats)       |
| `callback`  | Method name to use for JSONP callback  | none      | String                                |

### Defaults

If marked with an `*` the default shown can be changed by the administrator for the Atlas server. As a result
the default in the table may not match the default you see. The defaults listed do match those used for the
primary Atlas backends in use at Netflix.

For users running their own server, the config settings and corresponding query params are:

| Key                                   | Query Param |
|---------------------------------------|-------------|
| `atlas.webapi.graph.start-time`       | `s`         |
| `atlas.webapi.graph.end-time`         | `e`         |
| `atlas.webapi.graph.timezone`         | `tz`        |
| `atlas.webapi.graph.width`            | `w`         |
| `atlas.webapi.graph.height`           | `h`         |
| `atlas.webapi.graph.palette`          | `palette`   |

### Boolean Flags

Flags with a true or false value are specified using `1` for true and `0` for false.
