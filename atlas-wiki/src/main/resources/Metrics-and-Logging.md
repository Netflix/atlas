Atlas is intended for use with dimensional time-series data, however, one thing we often see is users trying to abuse it as a logging or event system. While there are some similarities, it is important to distinguish between these use-cases and use the more appropriate tool.

A time series is a set of values measured over time. An log message or event is represents a specific occurrence of something. This distinction is important for the types of queries and use-cases that can be supported efficiently by a given system.

## Uniqueness

Atlas is intended for showing time series that summarize events or behavior over time, not for finding or investigating a specific event.

## Granularity

## Log Parsing

It is a common use case to build log parsers to count events of a particular class (i.e. HTTP 2XX, HTTP 4XX) and send this data into Atlas.  The key here is being able to observe the trend of events being reported to logs.
