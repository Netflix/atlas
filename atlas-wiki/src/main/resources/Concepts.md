This page is meant to provide a basic overview of some of the key concepts and terminology that is
used for Atlas. If you have experience with other monitoring systems, then much of this should
be familiar though we might be using different terms.

## Time Series

Atlas is a backend for storing and querying dimensional time series data. A time series is a
sequence of [data points](#data-point) reported at a consistent interval over time. The time
interval between successive data points is called the [step size](#step-size). In Atlas each time
series is paired with metadata called [tags](#tags) that allow us to query and group the data.

## Tags

A set of key value pairs associated with a [time series](#time-series). Each time series must have
at least one tag with a key of `name`. To make it more concrete, here is an example of a tag set
represented as a JSON object:

```json
{
  "name":       "server.requestCount",
  "status":     "200",
  "endpoint":   "api",
  "nf.app":     "fooserver",
  "nf.cluster": "fooserver-main",
  "nf.stack":   "main",
  "nf.region":  "us-east-1",
  "nf.zone":    "us-east-1c",
  "nf.node":    "i-12345678"
}
```

Usage of tags will typically fall into two categories:

1. _Namespace:_ these are tags necessary to qualify a name so it can be meaningfully aggregated.
   For example, using the sample above consider computing the sum of all metrics for
   application `fooserver`. That number would be meaningless. Properly modelled data should try
   to make the aggregates meaningful by selecting the `name`. The sum of all metrics with
   `name = server.requestCount` is the overall request count for the service.
2. _Dimensions:_ these are tags used to drill down into the data, for example, see the number of
   requests with `status = 200` or for a particular node. Most tags should fall into this
   category.

When creating metrics it is important to carefully think about how the data should be tagged. See
the [naming conventions](https://github.com/Netflix/spectator/wiki/Conventions) docs for more
information.

## Metric

A metric is a specific quantity being measured, e.g., the number of requests received by a server.
In casual language about Atlas metric is often used interchangeably with
[time series](#time-series). A time series is one way to track a metric and is the method
supported by Atlas. In most cases there will be many time series for a given metric. Going back
to the example, request count would usually be tagged with additional dimensions such as status
and node. There is one time series for each distinct combination of tags, but conceptually it is
the same metric.

## Data Point

A data point is a triple consisting of tags, timestamp, and a value. It is important to understand
at a high level how data points correlate with the measurement. Consider requests hitting a
server, this would typically be measured using a
[counter](https://github.com/Netflix/spectator/wiki/Counter-Usage). Each time a request is received
the counter is incremented. There is not one data point per increment, a data point represents
the behavior over a span of time called the [step size](#step-size). The client library will
sample the counter once for each interval and report a single value.

Suppose that each circle in the diagram below represents a request:

```
1:00       1:01       1:02       1:03
 ├─●────●●●─┼──────────┼──●───────┤
```

There are 5 requests shown, 4 from 1:00 to 1:01, and 1 from 1:02 to 1:03. Assuming all requests
incremented the same time series, i.e. all other dimensions such as status code are the same, then
this would result in three data points. For counters values are always a rate per second, so for
a one minute step size the total number of requests would be divided by 60 seconds. So the values
stored would be:

| Time | Value           |
|------|-----------------|
| 1:01 | 4 / 60 = 0.0667 |
| 1:02 | 0 / 60 = 0.0000 |
| 1:03 | 1 / 60 = 0.0167 |

## Step Size

The amount of time between two successive data points in a [time series](#time-series). For Atlas
the datapoints will always be on even boundaries of the step size. If data is not reported on step
boundaries, it will get [normalized](#normalization) to the boundary.

## Normalization

In Atlas this usually refers to normalizing data points to step boundaries. Suppose that values
are actually getting reported at 30 seconds after the minute instead of exactly on the minute.
The values will get normalized to the minute boundary so that all time series in the system are
consistent.


How a normalized value is computed depends on the data source type. Atlas supports three types
indicated by the value of the `atlas.dstype` tag. In general, you should not need to worry about
that, client libraries like [spectator](https://github.com/Netflix/spectator/wiki) will
automatically handle tagging based on the data source type.

It is recommended to at least skim through the normalization for [gauges](#gauge) and
[rates](#rates) to better understand how the values you see actually relate to measured
data.

#### Gauge

A value that is sampled from some source and the value is used as is. The last value received will
be the value used for the interval. For example:

```
                ┌─────────┐                                    ┌─────────┐
                │    8    │                                    │    8    │
                │         ├───────                             │         │
                │         │    6                               │         │
──────┐         │         │                ┌─────────┐         │         │
 4    │         │         │                │    4    │         │         │
      ├─────────┤         │           to   │         ├─────────┤         │
      │    2    │         │                │         │    2    │         │
 ├─────────┼─────────┼─────────┤           ├─────────┼─────────┼─────────┤
1:00      1:01      1:02      1:03        1:00      1:01      1:02      1:03
```

#### Rate

A rate is a value representing the rate per second since the last reported value. Rate values
are normalized using a weighted average. For example:

```
                ┌─────────┐
                │    8    │                                    ┌─────────┐
                │         ├───────                             │    7    │
                │         │    6                     ┌─────────┤         │
──────┐         │         │                          │    5    │         │
 4    │         │         │                ┌─────────┤         │         │
      ├─────────┤         │           to   │    3    │         │         │
      │    2    │         │                │         │         │         │
 ├─────────┼─────────┼─────────┤           ├─────────┼─────────┼─────────┤
1:00      1:01      1:02      1:03        1:00      1:01      1:02      1:03
```

In this example, the data is reported at exactly 30s after the minute boundary. So each value
represents the average rate per second for 50% of the minute.

| Time | Value                         |
|------|-------------------------------|
| 1:01 | 4 * 0.5 + 2 * 0.5 = 2 + 1 = 3 |
| 1:02 | 2 * 0.5 + 8 * 0.5 = 1 + 4 = 5 |
| 1:03 | 8 * 0.5 + 6 * 0.5 = 4 + 3 = 7 |

If many samples are received for a given interval, then they will each be weighted based on the
fraction of the interval they represent. When no previous sample exists, value will be treated
as the average rate per second over the previous step. This behavior is important to avoid
under counting the contribution from a previous interval. The example below shows what happens
if there is no previous or next sample:

```
                ┌─────────┐
                │    8    │
                │         │
                │         │                          ┌─────────┐
                │         │                          │    5    ├─────────┐
                │         │                          │         │    4    │
      ┌─────────┤         │           to        1    │         │         │
      │    2    │         │                ┌─────────┤         │         │
 ├─────────┼─────────┼─────────┤           ├─────────┼─────────┼─────────┤
1:00      1:01      1:02      1:03        1:00      1:01      1:02      1:03
```

Why perform weighted averaging for rates instead of the simpler last value approach used with
gauges? Because it gives us a better summary of what we actually know from the measurements
received. In practical terms:

* It avoids dropping information if samples are more frequent than the step. Suppose we have
  a 1 minute step, but data is actually betting reported every 10s. For this example, assume
  we get 1, 5, 90, 5, 4, and 2. The last value normalization used with gauges would end up
  with a value of 2. The rate normalization will give 17.833. Each value is a rate per second,
  so if you take the `(1 + 5 + 90 + 5 + 4 + 2) * 10 = 1070` actual events measured during the
  interval. That is equivalent to `17.833 * 60` indicating we have an accurate average rate
  for the step size.
* Avoids skewing the data causing misleading spikes or drops in the aggregates. Using Atlas
  you will typically be looking at an aggregate of time series rather than an individual time
  series that was reported. With last value it can have the effect of skewing samples to a later
  interval. Suppose the client is reporting once a minute at 5s after the minute. That value
  indicates more about the previous interval than it does the current one. During traffic
  transitions, such as moving traffic over to a new cluster or even some auto-scaling events,
  differences in this skew can result in the appearance of a drop because there will be many
  new time series getting reported with a delayed start. For existing time series it is still
  skewed, but tends to be less noticeable. The weighted averaging avoids these problems for
  the most part.

#### Counter

Counter is similar to rate, except that the value reported is monotonically increasing and will
be converted to a rate by the backend. The conversion is done by computing the delta between the
current sample and the previous sample and dividing by the time between the samples. After that
it is the same as a [rate](#rate).

Note, that unless the input is a montonically increasing counter it is generally better to have
the client perform rate conversion. Since, the starting value is unknown, at least two samples
must be received before the first delta can be computed. This means that new time series relying
on counter type will be delayed by one interval.

## Aggregation Function

Combines a set of time series into a single time series. A conceptual measurement such as cpu
usage or requests per second. A metric will usually end up getting reported as a set of time
series, for example a cpu usage time series for each node.

## Consolidation Function

Converts a time series from a primary step size to a larger step size that is a multiple of the
primary. For example, if you have a time series with data points each minute and you want a time
series with data points every 5 minutes.

In most cases you won't use a consolidation function directly. The system will automatically apply
consolidation to ensure there is at least one pixel per datapoint on the rendered images.

## NaN Aware Operations

This typically refers to how binary operations treat NaN values in the data. A NaN value means
that the value is unknown for a given interval. NaN aware operations will keep the value as NaN
if both sides are NaN, but treat it as a special value such as 0.0 otherwise. This is useful to
avoid gaps in the graph if a subset of time series used in an aggregate have gaps in data.

| *lhs* | *rhs* | *result*     |
|-------|-------|--------------|
| NaN   | NaN   | op(NaN, NaN) |
| v1    | NaN   | op(v1, 0.0)  |
| NaN   | v2    | op(0.0, v2)  |
| v1    | v2    | op(v1, v2)   |

## Naming Conventions

See [naming conventions](https://github.com/Netflix/spectator/wiki/Conventions).




