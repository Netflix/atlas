## Description

Helper library for evaluating Atlas expressions. This library is still a work in
progress and for now is mostly focused on some of the streaming evaluation use-cases.

## Usage

The goal is to be able to simply create a [reactive streams publisher] from an Atlas
graph URI. For example:

[publisher]: https://github.com/reactive-streams/reactive-streams-jvm#1-publisher-code

```java
String uri = "http://localhost:7101/api/v1/graph?q=name,ssCpuUser,:eq,:avg";
Publisher<TimeSeriesMessage> publisher = Evaluator.createPublisher(uri);
```

This can then be consumed using any implementation of reactive streams.