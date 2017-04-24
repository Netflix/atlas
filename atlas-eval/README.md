## Description

Helper library for evaluating Atlas expressions. This library is still a work in
progress and for now is mostly focused on some of the streaming evaluation use-cases.

## Usage

The public api is based around the `com.netflix.eval.stream.Evaluator` class. To
create a new instance. Normally this would be done via the injector, but a simple
standalone instance can be done manually like:

```java
Config config = ConfigFactory.load();
Registry registry = new DefaultRegistry();
ActorSystem system = ActorSystem.create("eval", config);
Evaluator evaluator = new Evaluator(config, registry, system);
```

### Configuration

The set of supported backends needs to be setup in the config by setting the
`atlas.eval.stream.backends` property. The default reference config only has
an entry for localhost.

### Single URI Publisher

Create a [reactive streams publisher] from an Atlas graph URI. For example:

[publisher]: https://github.com/reactive-streams/reactive-streams-jvm#1-publisher-code

```java
String uri = "http://localhost:7101/api/v1/graph?q=name,ssCpuUser,:eq,:avg";
Publisher<TimeSeriesMessage> publisher = evaluator.createPublisher(uri);
```

This can then be consumed using any implementation of reactive streams.

### Dynamic Set Processor

Create a processor that takes the `DataSources` as input and outputs `MessageEnvelope`
objects for the matching set. This method should be preferred for use-cases where many
expressions need to be evaluated so the connections and other resources can be shared.

```java
Processor<Evaluator.DataSources, Evaluator.MessageEnvelope> processor =
  evaluator.createStreamsProcessor();
```

Each `DataSource` for the input has a string id that will be added to corresponding
`MessageEnvelope` objects in the output. This allows the messages to be matched with
the correct input by the user.
