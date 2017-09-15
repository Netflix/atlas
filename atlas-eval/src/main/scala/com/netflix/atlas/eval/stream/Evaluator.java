/*
 * Copyright 2014-2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.atlas.eval.stream;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.ThrottleMode;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Framing;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;
import akka.util.ByteString;
import com.netflix.atlas.json.JsonSupport;
import com.netflix.spectator.api.NoopRegistry;
import com.netflix.spectator.api.Registry;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import javax.inject.Inject;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Public API for streaming evaluation of expressions.
 */
public final class Evaluator extends EvaluatorImpl {

  /**
   * Create a new instance. It is assumed this is created via the injector and the
   * signature may change over time.
   */
  @Inject
  public Evaluator(Config config, Registry registry, ActorSystem system) {
    super(config, registry, system);
  }

  /**
   * Collects the data from LWC for the specified duration and store it to a file. The
   * file can then be used as an input to replay evaluation.
   *
   * @param uri
   *     Source URI for collecting the data.
   * @param file
   *     Output file to store the results.
   * @param duration
   *     Specified how long the collection will run. Note this method will block until
   *     the collection is complete.
   */
  public void writeInputToFile(String uri, Path file, Duration duration) {
    writeInputToFileImpl(uri, file, duration);
  }

  /**
   * Creates a publisher stream for a given URI.
   *
   * @param uri
   *     Source URI for collecting the data.
   * @return
   *     Publisher that produces events representing the evaluation results for the
   *     expression in the URI.
   */
  public Publisher<JsonSupport> createPublisher(String uri) {
    return createPublisherImpl(uri);
  }

  /**
   * Creates a processor that can multiplex many streams at once. This can be used to
   * help reduce the overhead in terms of number of connections and duplicate work on
   * the backend producing the data.
   *
   * It takes a stream of data sources as an input and returns the output of evaluating
   * those streams. Each {@link DataSources} object should be the complete set of
   * sources that should be evaluated at a given time. The output messages can be
   * correlated with a particular data source using the id on the {@link MessageEnvelope}.
   */
  public Processor<DataSources, MessageEnvelope> createStreamsProcessor() {
    return createStreamsProcessorImpl();
  }

  /**
   * Immutable set of data sources that should be consumed.
   */
  public final static class DataSources {
    private final Set<DataSource> sources;

    /** Create a new instance that is empty. */
    public static DataSources empty() {
      return new DataSources(Collections.emptySet());
    }

    /** Create a new instance. */
    public static DataSources of(DataSource... sources) {
      Set<DataSource> set = new HashSet<>(Arrays.asList(sources));
      return new DataSources(set);
    }

    /** Create a new instance. */
    public DataSources(Set<DataSource> sources) {
      this.sources = Collections.unmodifiableSet(new HashSet<>(sources));
    }

    /** Return the data sources in this set. */
    public Set<DataSource> getSources() {
      return sources;
    }

    /** Compares with another set and returns the new data sources that have been added. */
    public Set<DataSource> addedSources(DataSources other) {
      Set<DataSource> copy = new HashSet<>(sources);
      copy.removeAll(other.getSources());
      return copy;
    }

    /** Compares with another set and returns the data sources that have been removed. */
    public Set<DataSource> removedSources(DataSources other) {
      return other.addedSources(this);
    }

    DataSources remoteOnly() {
      Set<DataSource> remote = new HashSet<>(sources);
      remote.removeAll(localSources());
      return new DataSources(remote);
    }

    DataSources localOnly() {
      return new DataSources(localSources());
    }

    private Set<DataSource> localSources() {
      return sources.stream()
          .filter(DataSource::isLocal)
          .collect(Collectors.toSet());
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DataSources that = (DataSources) o;
      return sources.equals(that.sources);
    }

    @Override public int hashCode() {
      return sources.hashCode();
    }

    @Override public String toString() {
      return sources.stream()
          .map(DataSource::toString)
          .collect(Collectors.joining(",", "DataSources(", ")"));
    }
  }

  /**
   * Pair mapping an id to a URI that should be consumed. The id can be used to find the
   * messages in the output that correspond to this data source.
   */
  public final static class DataSource {
    private final String id;
    private final String uri;

    /** Create a new instance. */
    public DataSource(String id, String uri) {
      this.id = id;
      this.uri = uri;
    }

    /** Returns the id for this data source. */
    public String getId() {
      return id;
    }

    /** Returns the URI for this data source. */
    public String getUri() {
      return uri;
    }

    /** Returns true if the URI is for a local file or classpath resource. */
    public boolean isLocal() {
      return uri.startsWith("/")
          || uri.startsWith("file:")
          || uri.startsWith("resource:");
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DataSource that = (DataSource) o;
      return id.equals(that.id) && uri.equals(that.uri);
    }

    @Override public int hashCode() {
      int result = id.hashCode();
      result = 31 * result + uri.hashCode();
      return result;
    }

    @Override public String toString() {
      return "DataSource(" + id + "," + uri + ")";
    }
  }

  /**
   * Wraps the output messages from the evaluation with the id of the data source. This
   * can be used to route the message back to the appropriate consumer.
   */
  public final static class MessageEnvelope {
    private final String id;
    private final JsonSupport message;

    /** Create a new instance. */
    public MessageEnvelope(String id, JsonSupport message) {
      this.id = id;
      this.message = message;
    }

    /** Returns the id for the data source. */
    public String getId() {
      return id;
    }

    /** Returns the message. */
    public JsonSupport getMessage() {
      return message;
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      MessageEnvelope that = (MessageEnvelope) o;
      return id.equals(that.id) && message.equals(that.message);
    }

    @Override public int hashCode() {
      int result = id.hashCode();
      result = 31 * result + message.hashCode();
      return result;
    }

    @Override public String toString() {
      return "MessageEnvelope(" + id + "," + message + ")";
    }
  }

  /**
   * Helper used for simple tests. A set of URIs will be read from {@code stdin}, one per line,
   * and sent through a processor created via {@link #createStreamsProcessor()}. The output will
   * be sent to {@code stdout}.
   *
   * @param args
   *     Paths to additional configuration files that should be loaded. The configs will be
   *     loaded in order with later entries overriding earlier ones if there are conflicts.
   */
  public static void main(String[] args) throws Exception {
    final Logger logger = LoggerFactory.getLogger(Evaluator.class);

    // Load any additional configurations if present
    Config config = ConfigFactory.load();
    for (String file : args) {
      logger.info("loading configuration file {}", file);
      config = ConfigFactory.parseFile(new File(file))
          .withFallback(config)
          .resolve();
    }

    // Setup evaluator
    ActorSystem system = ActorSystem.create();
    ActorMaterializer mat = ActorMaterializer.create(system);
    Evaluator evaluator = new Evaluator(config, new NoopRegistry(), system);

    // Process URIs
    StreamConverters                                       // Read in URIs from stdin
        .fromInputStream(() -> System.in)
        .via(Framing.delimiter(ByteString.fromString("\n"), 16384))
        .map(b -> b.decodeString(StandardCharsets.UTF_8))
        .zipWithIndex()                                    // Use line number as id for output
        .map(p -> new DataSource(p.second().toString(), p.first()))
        .fold(new HashSet<DataSource>(), (vs, v) -> { vs.add(v); return vs; })
        .map(DataSources::new)
        .flatMapConcat(Source::repeat)                     // Repeat so stream doesn't shutdown
        .throttle(                                         // Throttle to avoid wasting CPU
            1, FiniteDuration.apply(1, TimeUnit.MINUTES),
            1, ThrottleMode.shaping()
        )
        .via(Flow.fromProcessor(evaluator::createStreamsProcessor))
        .runForeach(
            msg -> System.out.printf("%10s: %s%n", msg.getId(), msg.getMessage().toJson()),
            mat
        )
        .toCompletableFuture()
        .get();
  }
}
