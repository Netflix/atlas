/*
 * Copyright 2014-2025 Netflix, Inc.
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

import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.http.javadsl.model.Uri;
import org.apache.pekko.stream.Materializer;
import org.apache.pekko.stream.ThrottleMode;
import org.apache.pekko.stream.javadsl.Flow;
import org.apache.pekko.stream.javadsl.Framing;
import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.stream.javadsl.StreamConverters;
import org.apache.pekko.util.ByteString;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.netflix.atlas.core.util.Strings$;
import com.netflix.atlas.json.JsonSupport;
import com.netflix.spectator.api.NoopRegistry;
import com.netflix.spectator.api.Registry;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Public API for streaming evaluation of expressions.
 */
public final class Evaluator extends EvaluatorImpl {

  /**
   * Create a new instance. It is assumed this is created via the injector and the
   * signature may change over time.
   */
  public Evaluator(Config config, Registry registry, ActorSystem system) {
    super(config, registry, system, Materializer.createMaterializer(system));
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
   * <p>It takes a stream of data sources as an input and returns the output of evaluating
   * those streams. Each {@code DataSources} object should be the complete set of
   * sources that should be evaluated at a given time. The output messages can be
   * correlated with a particular data source using the id on the {@code MessageEnvelope}.
   */
  public Processor<DataSources, MessageEnvelope> createStreamsProcessor() {
    return createStreamsProcessorImpl();
  }

  /**
   * Creates a processor that evaluates a fixed set of expressions against the stream of
   * time grouped datapoints. The caller must ensure:
   *
   * <ul>
   *   <li>All data sources have the same step size. This is required because there is a
   *       single input datapoint stream with a fixed step.</li>
   *   <li>There should be a single group for each step interval with timestamps aligned to
   *       the step boundary.</li>
   *   <li>Operations are feasible for the input. Time based operators may or may not work
   *       correctly depending on how the timestamps for the input groups are determined.
   *       For use-cases where the the timestamps are generated arbitrarily, such as incrementing
   *       for each group once a certain number of datapoints is available, then operators
   *       based on a clock such as {@code :time} will have bogus values based on the arbitrary
   *       times for the group. If the data is a snapshot of an actual stream, then time based
   *       operators should work correctly.</li>
   * </ul>
   */
  public Processor<DatapointGroup, MessageEnvelope> createDatapointProcessor(DataSources sources) {
    return createDatapointProcessorImpl(sources);
  }

  /**
   * Perform static analysis checks to ensure that the provided data source is supported
   * by this evaluator instance.
   *
   * @param ds
   *     Data source to validate.
   */
  public void validate(DataSource ds) {
    validateImpl(ds);
  }

  /**
   * Immutable set of data sources that should be consumed.
   */
  public record DataSources(Set<DataSource> sources) {

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
    public DataSources {
      sources = Set.copyOf(sources);
    }

    /** Compares with another set and returns the new data sources that have been added. */
    public Set<DataSource> addedSources(DataSources other) {
      Set<DataSource> copy = new HashSet<>(sources);
      copy.removeAll(other.sources());
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

    /**
     * Return the step size for the sources in this set. If there are mixed step sizes an
     * IllegalStateException will be thrown. If the set is empty, then -1 will be returned.
     */
    long stepSize() {
      long step = -1L;
      for (DataSource source : sources) {
        long sourceStep = source.step().toMillis();
        if (step != -1L && step != sourceStep) {
          throw new IllegalStateException("inconsistent step sizes, expected "
              + step + ", found " + sourceStep + " on " + source);
        }
        step = sourceStep;
      }
      return step;
    }
  }

  /**
   * Triple mapping an id to a step size and a URI that should be consumed. The
   * id can be used to find the messages in the output that correspond to this
   * data source.
   *
   * @param id
   *     An identifier for this {@code DataSource}. It will be added to
   *     corresponding MessageEnvelope objects in the output, facilitating
   *     matching output messages with the data source.
   * @param step
   *     The requested step size for this {@code DataSource}. <em>NOTE:</em>
   *     This may result in rejection of this {@code DataSource} if the
   *     backing metrics producers do not support the requested step size.
   * @param uri
   *     The URI for this {@code DataSource} (in atlas backend form).
   */
  public record DataSource(String id, Duration step, String uri) {

    /**
     * Create a new instance.
     */
    public DataSource {
      step = step == null ? extractStepFromUri(uri) : step;
    }

    /**
     * Create a new instance with the step size being derived from the URI or falling
     * back to {@link #DEFAULT_STEP}.
     *
     * @param id
     *     An identifier for this {@code DataSource}. It will be added to
     *     corresponding MessageEnvelope objects in the output, facilitating
     *     matching output messages with the data source.
     * @param uri
     *     The URI for this {@code DataSource} (in atlas backend form).
     */
    public DataSource(String id, String uri) {
      this(id, null, uri);
    }

    /** Returns true if the URI is for a local file or classpath resource. */
    @JsonIgnore
    public boolean isLocal() {
      return uri.startsWith("/")
          || uri.startsWith("file:")
          || uri.startsWith("resource:")
          || uri.startsWith("synthetic:");
    }
  }

  /**
   * Wraps the output messages from the evaluation with the id of the data source. This
   * can be used to route the message back to the appropriate consumer.
   */
  public record MessageEnvelope(String id, JsonSupport message) {
  }

  /**
   * Group of datapoints for the same time.
   */
  public record DatapointGroup(long timestamp, List<Datapoint> datapoints) {
  }

  /**
   * Represents a datapoint for a dataset that is generated independently of the LWC clusters.
   * This can be used for synthetic data or if there is an alternative data source such as a
   * stream of events that are being mapped into streaming time series.
   */
  public record Datapoint(Map<String, String> tags, double value) {
  }

  /** The default step size. */
  private static final Duration DEFAULT_STEP = Duration.ofSeconds(60L);

  /**
   * Try to extract the step size based on the URI query parameter. If this fails for any
   * reason or the parameter is not present, then use the default of 1 minute.
   */
  private static Duration extractStepFromUri(String uriString) {
    try {
      Uri uri = Uri.create(uriString, Uri.RELAXED);
      return uri.query()
          .get("step")
          .map(Strings$.MODULE$::parseDuration)
          .orElse(DEFAULT_STEP);
    } catch (Exception e) {
      return DEFAULT_STEP;
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
    Materializer mat = Materializer.createMaterializer(system);
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
            1, Duration.ofMinutes(1),
            1, ThrottleMode.shaping()
        )
        .via(Flow.fromProcessor(evaluator::createStreamsProcessor))
        .runForeach(
            msg -> System.out.printf("%10s: %s%n", msg.id(), msg.message().toJson()),
            mat
        )
        .toCompletableFuture()
        .get();
  }
}
