## Overview

Atlas Stack Language is designed to be a stable method of representing complex data queries in a URL-friendly format. It is loosely based on the [RPN expressions](http://oss.oetiker.ch/rrdtool/doc/rrdgraph_rpn.en.html) supported by [Tobias Oetiker](https://tobi.oetiker.ch/hp/)'s [rrdtool](http://oss.oetiker.ch/rrdtool/).  The language uses postfix expressions with comma-separated arguments and all commands begin with a colon. Lists can be created by using an open parenthesis '(' to start the list and a closing parenthesis ')' to end the list and commands are provided which can iterate over lists.  The parentheses punctuation is also used separately as a part of `:re` expressions, in which case it is not separated by commas.

The following is an example of a stack language expression:

`nf.cluster,discovery,:eq,(,nf.zone,),:by`

This example pushes two strings nf.cluster and discovery onto the stack and then executes the command :eq. The equal command pops two strings from the stack and pushes a query object onto the stack.  The behavior can be described by the stack effect String:key String:value – Query.  We then push a list of tag keys to the stack and execute the command :by to group the results.

## Parts

* Punctuation: `,():`

## Data Model

Set of tags

Constructing a query expression has a general flow of:

1. Query
2. Aggregation
3. Math
4. Presentation

## Matching

Consider an environment where you have two clusters: a web serving cluster and a database cluster.

```
lb -> web1 -> database1
   -> web2 -> database2
```

Common attributes:

* app: 
* cluster: a group of related machines
* asg: scaling group
* region: Amazon region
* zone: Amazon availability zone

System metrics:

* cpu: example of a common system metric
* request: 
    * status
    * statistic
    * method
    * endpoint


```
[
  {"app": "web", "cluster": "web-main", "asg": "web-main-v001", "region": "us-east-1", "zone": "us-east-1a", "name": "cpu"}
]
```

### Query

The query is used to select a set of time series. The primary query operators are:

* eq
* and

For more details see the stack language reference page.

```
app,web,:eq
```

This query will match all time series where the key `app` is equal to `web`. If you are familiary with SQL and assume that tag keys are column names, then this would be equivalent to:

```
select * from ts where app = 'web';
```

Using the example data set this query would return the following subset:



### Aggregation

An aggregation function maps a set of time series that matched the query to a single time series. The following aggregates are supported:

```
app,web,:eq,name,cpu,:eq,:and,:sum
```

```
app,web,:eq,name,cpu,:eq,:and,:avg
```

TODO: Group by

### Math

```
app,web,:eq,name,cpu,:eq,:and,
:dup,
:sum,
:swap,
:count,
:div
```

```
SUM(q1) / COUNT(q1)
```

### Presentation

* legend
* color
* line width

## Manipulating the Stack