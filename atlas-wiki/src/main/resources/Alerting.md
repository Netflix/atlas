The stack langauge provides some basic techniques to convert an input line into a set of signals that can be used to trigger and visualize alert conditions. This section assumes a familiarity with the stack language and the [alerting philosphy](Alerting-Philosophy)

## Example Application

All of the sample alerts in this document will be based off a simple example application. The application consists of two clusters:

1. `www`: a stateless web server cluster that processes HTTP requests.
2. `database`: a stateful cluster used by the `www` cluster.

For this example, `server.requests` is the number of requests per second hitting the `www` server. It has a `status` dimension which can be used to break down the requests by the HTTP status code.

## Threshold Alerts

### Single line

A simple alert consists of comparing an input signal with a threshold and in some cases checking for a certain number of occurrences before triggering. Suppose that our base query is:

```
nf.cluster,www,:eq,
name,server.requests,:eq,:and,
status,(,4xx,5xx,),:in,:and,
:sum,
errors,:swap,:set
```

TODO: sample chart

This query creates a line showing the number of errors, 4xx or 5xx status code, per second across all instances of cluster `www`. To convert this into a signal line that indicates a problem has occurred we can use one of the comparison operators like [[:gt|Stack-Language-Reference#gt-1]].

```
errors,:get,4,:gt
```

This expression creates a line that is 1 for intervals where the number of errors per second is greater than 4 and 0 for intervals where it is less than or equal to 4. 

TODO: sample chart

### Signal and Visualization



### Multiple line

### Number of Occurrences

## Trend Deviation Alerts

### Trend

### DES

The values of alpha and beta that i have settled on are categorized by "smoothed signal response time", from fast to slow. fast response will adapt to changing levels in the underlying signal quickly, slow will adapt ... slowly. 
fast:  alpha=0.1, beta=0.02
slower: alpha=0.05, beta=0.03
slow: alpha=0.03, beta=0.04
These values for alpha and beta reduce the oscillations in the smoothed signal.

### Week over Week

## Outlier Alerts

## Examples

* Request errors (client, server, regular noise)
* Cache age, ignore OOS nodes
* Stale cache getting updated 