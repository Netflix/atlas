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

[reactive streams publisher]: https://github.com/reactive-streams/reactive-streams-jvm#1-publisher-code

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

### Data Rate Messages
The library also emits data rate messages to help gain insights in the data rate per `DataSource` 
per step. There are 3 type of data sizes in a data rate message:
 - Input size: number of data points coming in as raw input.
 - Intermediate size: number of aggregate data points that are used in the eval step.
 - Output size: number of data points for the final result.

The "size" has the 2 fields: 
 - `total`: total number of data points 
 - `details`: number of data points per data expression (empty for "outputSize")

For example, given a query of "app,www,:eq,name,request,:eq,:and,:sum,(,cluster,),:by" with 4 nodes
and 2 clusters, one of the data rate message evelope may look like this:
```json5
{
  "id": "_",
  "message": {
    "type": "rate",
    "timestamp": 1596570660000,
    "step": 60000,
    "inputSize": {
      "total": 4,
      "details": {
        "app,www,:eq,name,request,:eq,:and,:sum,(,cluster,),:by": 4
      }
    },
    "intermediateSize": {
      "total": 2,
      "details": {
        "app,www,:eq,name,request,:eq,:and,:sum,(,cluster,),:by": 2
      }
    },
    "outputSize": {
      "total": 2,
      "details": {}
    }
  }
}
```