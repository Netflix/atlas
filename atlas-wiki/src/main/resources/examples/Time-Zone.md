> [[Home]] ▸ Examples

Examples for specifying the time zone:

* [Single Zone](#single-zone)
* [Multi Zone](#multi-zone)
* [Daylight Savings Time](#daylight-savings-time)

## Single Zone

Most graphs will only show a single time zone. By default the zone is `US/Pacific`. To set to
another zone such as `UTC` use the `tz` query parameter:

/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq&tz=UTC

## Multi Zone

The `tz` parameter can be specified multiple times in which case one X-axis will be shown per
zone. Start and end times will be based on the first time zone listed.

/api/v1/graph?e=2012-01-01T00:00&s=e-2d&q=name,sps,:eq&tz=UTC&tz=US/Pacific&tz=US/Eastern

## Daylight Savings Time

If using a time zone that changes for daylight savings time, then you will see duplicate or missing
hours on the time axis labels during the transition period. For example, a duplicate hour:

/api/v1/graph?e=2015-11-01T08:00&s=e-12h&q=name,sps,:eq&tz=US/Pacific&tz=UTC

A missing hour:

/api/v1/graph?e=2015-03-08T08:00&s=e-12h&q=name,sps,:eq&tz=US/Pacific&tz=UTC

If looking at a longer time frame, then it can also throw off the alignment so ticks will not
be on significant time boundaries, e.g.:

/api/v1/graph?e=2015-11-05T08:00&s=e-1w&q=name,sps,:eq&tz=US/Pacific&tz=UTC
