> [[Home]] ▸ Examples

This section gives some examples to get started quickly creating simple graphs.

* [Single Line](#single-line)
* [Adding a Title](#adding-a-title)
* [Multiple Lines](#multiple-lines)
* [Group By](#group-by)
* [Simple Math](#simple-math)
* [Binary Operations](#binary-operations)

## Single Line

The only required parameter is `q` which specifies the [query expression](Stack-Langauge) for
a line. The other two common parameters are for setting the start time, `s`, and the end time, `e`,
for the data being shown. Usually the start time will be set relative to the end time, such as
`e-3h`, which indicates 3 hours before the end time. See [time parameters](Time-Parameters) for
more details on time ranges.

Putting it all together:

/api/v1/graph?s=e-2d&e=2012-01-01T00:00&q=name,sps,:eq

## Adding a Title

The graph title can be set using the `title` parameter. Similarly, a Y-axis label can be set
using the `ylabel` parameter.

/api/v1/graph?s=e-2d&e=2012-01-01T00:00&q=name,sps,:eq&title=Starts+Per+Second&ylabel=sps

## Multiple Lines

Multiple expressions can be placed on a chart by concatenating the expressions, e.g., showing
a query expression along with a constant value:

/api/v1/graph?s=e-2d&e=2012-01-01T00:00&q=name,sps,:eq,500e3

## Group By

Multiple lines can also be a result of a single expression via [group by](data-by).

/api/v1/graph?s=e-2d&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by

## Simple Math

A number of operators are provided to manipulate a line. See the math section of the
[stack language reference](Stack-Language-Reference) for a complete list.
Example that [negates](math-neg) the value of a line: 

/api/v1/graph?s=e-2d&e=2012-01-01T00:00&q=name,sps,:eq,:neg

Example that [negates](math-neg) and then applies [absolute value](math-abs) to get the
original value back (since all values were positive in the input): 

/api/v1/graph?s=e-2d&e=2012-01-01T00:00&q=name,sps,:eq,:neg,:abs

## Binary Operations

Lines can be combined using binary math operators such as [add](math-add) or [multiply](math-mul).
Example using [divide](math-div):

/api/v1/graph?s=e-2d&e=2012-01-01T00:00&q=name,sps,:eq,1000,:div

If used with a group by, then either:

* Both sides have the same group by clause. In this case an inner join will be preformed and the
  binary operation will be applied to the corresponding entries from both sides.
* One side is not a grouped expression, and the binary operation will be applied for each instance
  in the grouped result set.

### Both Sides Grouped

Dividing by self with both sides grouped:

/api/v1/graph?s=e-2d&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by,:dup,:div
  
### One Side Grouped

Dividing a grouped expression by a constant:

/api/v1/graph?s=e-2d&e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by,1000,:div

Equivalent to the previous expression, but the right-hand side is grouped and it uses multiply
instead of divide:

/api/v1/graph?s=e-2d&e=2012-01-01T00:00&q=0.001,name,sps,:eq,(,nf.cluster,),:by,:mul
