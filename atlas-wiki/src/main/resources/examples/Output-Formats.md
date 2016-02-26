> [[Home]] ▸ Examples

The following output formats are supported by default for [graphing](Graph):

* [png](#png)
* [csv](#csv)
* [txt](#txt)
* [json](#json)
* [std.json](#stdjson)
* [stats.json](#statsjson)

## png

This is the default and creates a PNG image for the graph. The mime type is `image/png`.

/api/v1/graph?q=hourOfDay,:time,minuteOfHour,:time,NaN&e=2012-01-01T09:00&s=e-3m&tz=UTC&format=png

## csv

Comma separated value output. The mime type is `text/csv`.

/api/v1/graph?q=hourOfDay,:time,minuteOfHour,:time,NaN&e=2012-01-01T09:00&s=e-5m&tz=UTC&format=csv

## txt

Same as [csv](#csv) except that the separator is a tab character instead of a comma. The mime
type will be `text/plain` so it is more likely to render directly in the browser rather than
trigger a download.

/api/v1/graph?q=hourOfDay,:time,minuteOfHour,:time,NaN&e=2012-01-01T09:00&s=e-5m&tz=UTC&format=txt

## json

JSON output representing the data. Note that it is not [standard json](http://json.org) as numeric
values like `NaN` will not get quoted.

/api/v1/graph?q=hourOfDay,:time,minuteOfHour,:time,NaN&e=2012-01-01T09:00&s=e-5m&tz=UTC&format=json

## std.json

Same as [json](#json) except that numeric values which are not recognized by
[standard json](http://json.org) will be quoted. The mime type is `application/json`.

/api/v1/graph?q=hourOfDay,:time,minuteOfHour,:time,NaN&e=2012-01-01T09:00&s=e-5m&tz=UTC&format=std.json

## stats.json

Provides the summary stats for each line, but not all of the data points. The mime type is
`application/json`.

/api/v1/graph?q=hourOfDay,:time,minuteOfHour,:time,NaN&e=2012-01-01T09:00&s=e-5m&tz=UTC&format=stats.json
