APIs that accept time ranges support three parameters:

1. Start time (`s`)
2. End time (`e`)
3. Time zone (`tz`)

## Time Zone

Time zone can be any valid [time zone id](Time-Zones) string.

## Time

### Absolute Times

Absolute times can be specified by [name](#named-times) or as a [timestamp](#timestamps). 

#### Named Times

Named times are references that will get resolved to a timestamp when a query is
executed. For example, with graphs it is common to set the end time to `now`.

| Name | Description |
|------|-------------|
| `s` | User specified start time. Can only be used as part of the end parameter. |
| `e` | User specified end time. Can only be used as part of the start parameter. |
| `now` | Current time. |
| `epoch` | January 1, 1970 UTC. |

#### Timestamps

Explicit timestamps can use the following [formats](http://pubs.opengroup.org/onlinepubs/009695399/functions/strftime.html):

| Format | Description |
|--------|-------------|
| %Y-%m-%d  | Date using the timezone for the query. The time will be 00:00. |
| %Y-%m-%dT%H:%M | Date time using the timezone for the query. The seconds will be 00. |
| %Y-%m-%dT%H:%M:%S | Date time using the timezone for the query. |
| %s | Seconds since January 1, 1970 UTC. |
| %s (ms) | Milliseconds since January 1, 1970 UTC. |

For times since the epoch both seconds and milliseconds are supported because
both are in common use and it helps to avoid confusion when copy and pasting
from another source. Values less than or equal 2,147,483,648 (2<sup>31</sup>)
will be treated as a timestamp in seconds. Values above that will be
treated as a timestamp in milliseconds. So times from the epoch to
1970-01-25T20:31:23 cannot be represented in the millisecond form. In practice,
this limitation has not been a problem.

The first three formats above can also be used with an [explicit time zone](#zone-offsets).

#### Zone Offsets

An explicit time zone can be specified as `Z` to indicate UTC or by using an offset
in hours and minutes. For example:

```
2012-01-12T01:37Z
2012-01-12T01:37-00:00
2012-01-12T01:37-07:00
2012-01-12T01:37-07:42
```

A common format recommended for logs at Netflix is an ISO timestamp in UTC:

```
2012-01-12T01:37:27Z
```

These can be copy and pasted to quickly check a graph for a timestamp from a log
file. For practical purposes in Atlas a `-00:00` offset timezone can be thought of
as UTC, but depending on the source may have some
[additional meaning](https://tools.ietf.org/html/rfc3339#section-4.3).

### Relative Times

Relative times consist of a [named time](#named-times) used for an anchor and
an offset duration.

```
<named=time> '-' <duration>
<named-time> '+' <duration>
```

For example:

| Pattern | Description |
|---------|-------------|
| now-1w  | One week ago. |
| e-1w    | One week before the end time. |
| s+6h    | Six hours after the start time. |
| s+P2DT6H5M | Two days, 6 hours, and 5 minutes after the start time. |

#### Durations

##### Duration vs Period

This section is using the definition of duration and period from the
[java time libraries](https://docs.oracle.com/javase/tutorial/datetime/iso/period.html). In short:

* Durations are a fixed number of seconds.
* Periods represent a length of time in a given calendar. For example, the length of
  a day will vary if there is a daylight savings transition.

The offset used for relative times in Atlas are durations because:

*  It is
  primarily focused on shorter time spans (~ 2 weeks) where drift is less of
  an issue. In this range the variation is most commonly seen for the daylight
  savings time transitions. 
* For time shifts day over day and week over week are the most common for
  operational purposes. During daylight savings time transitions a fixed duration
  seems to cause the least confusion, especially when the transition time is within
  the window being displayed. The primary use-case where periods were found to be
  more beneficial and less confusing is for week over week when looking at a
  small window that does not include the transition. In those cases if the signal
  reflects human behavior, such as playing movies, then the week over week pattern
  will typically line up better if using a period. 

##### Simple Duration

A simple offset uses a positive integer followed by one of these units:

* `s`, `second`, or `seconds`
* `m`, `min`, `minute`, or `minutes`
* `h`, `hour`, or `hours`
* `d`, `day`, or `days`
* `w`, `week`, or `weeks`
* `month` or `months`
* `y`, `year`, or `years`

All durations are a fixed number of seconds. A day is 24 hours, week is 7 days,
month is 30 days, and a year is 365 days.

##### ISO Duration

The duration can also be specified as an
[ISO duration string](https://tools.ietf.org/html/rfc3339#appendix-A), but day (`D`)
is the largest part that can be used within the duration. Others such as week (`W`),
month (`M`), and year (`Y`) are not supported. Examples:

| Pattern | Description |
|---------|-------------|
| P1D  | One day of exactly 24 hours. |
| P1DT37M    | One day and 37 minutes. |
| PT5H6M    | Five hours and six minutes. |

For more details see docs on [parsing durations](https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-).
