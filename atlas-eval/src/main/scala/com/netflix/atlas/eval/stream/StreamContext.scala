/*
 * Copyright 2014-2026 Netflix, Inc.
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
package com.netflix.atlas.eval.stream

import java.nio.file.Path
import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.stream.IOResult
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.FileIO
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.scaladsl.StreamConverters
import org.apache.pekko.util.ByteString
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.eval.model.ExprType
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.json3.JsonSupport
import com.netflix.atlas.pekko.DiagnosticMessage
import com.netflix.atlas.pekko.PekkoHttpClient
import com.netflix.atlas.pekko.StreamOps
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

private[stream] class StreamContext(
  rootConfig: Config,
  val materializer: Materializer,
  val registry: Registry = new NoopRegistry,
  val dsLogger: DataSourceLogger = DataSourceLogger.Noop
) extends StrictLogging {

  import StreamContext.*

  val id: String = UUID.randomUUID().toString

  private val config = rootConfig.getConfig("atlas.eval.stream")

  // Number of messages that could not be matched to an active data source expression and
  // were dropped. Some churn is expected while the set of data sources is being updated, but
  // a sustained non-zero rate indicates data loss, e.g. from a producer and consumer that
  // disagree on how an expression is rendered.
  private val unmatchedMessages = registry.counter("atlas.eval.unmatchedMessages")

  private val backends = {
    import scala.jdk.CollectionConverters.*
    config.getConfigList("backends").asScala.toList.map { cfg =>
      // URI for Edda service. For backwards compatibility Eureka name is also
      // checked.
      val eddaUri = cfg.getString("edda-uri")
      val checkForExpensiveQueries = isNotFalse(cfg, "check-for-expensive-queries")
      EddaBackend(
        cfg.getString("host"),
        eddaUri,
        checkForExpensiveQueries
      )
    }
  }

  private def isNotFalse(cfg: Config, k: String): Boolean = {
    if (cfg.hasPath(k))
      cfg.getBoolean(k)
    else
      true
  }

  private val ignoredTagKeys = {
    import scala.jdk.CollectionConverters.*
    config.getStringList("ignored-tag-keys").asScala.toSet
  }

  def numBuffers: Int = config.getInt("num-buffers")

  // Maximum number of raw input data points for a data expr.
  def maxInputDatapointsPerExpression: Int = config.getInt("limits.max-input-datapoints")

  // Maximum number of datapoints resulting from a group by for a data expr.
  def maxIntermediateDatapointsPerExpression: Int =
    config.getInt("limits.max-intermediate-datapoints")

  // Maximum allowed step size for a stream
  private val maxStep = config.getDuration("limits.max-step")

  val interpreter = new ExprInterpreter(rootConfig)

  def findBackendForUri(uri: Uri): Backend = {
    if (uri.isRelative || uri.scheme == "file")
      FileBackend(Paths.get(uri.path.toString()))
    else if (uri.scheme == "resource")
      ResourceBackend(uri.path.toString().substring(1))
    else if (uri.scheme == "synthetic")
      SyntheticBackend(interpreter, uri)
    else
      findEurekaBackendForUri(uri)
  }

  def localSource(uri: Uri): Source[ByteString, Future[IOResult]] = {
    findBackendForUri(uri).source
  }

  def findEurekaBackendForUri(uri: Uri): EddaBackend = {
    val host = uri.authority.host.address()
    backends.find(_.host == host) match {
      case Some(backend) => backend
      case None          => throw new NoSuchElementException(host)
    }
  }

  /**
    * State for the data sources being processed by the stream, tracked per backend host.
    * `createStreamsFlow` groups data sources by host and runs a per-host processor over this
    * shared context, so each host must update its own state independently. Partitioning by host
    * prevents one host from clobbering another's data sources and lets message delivery be scoped
    * to the host that produced the data. Each host's data sources and derived expression map are
    * held together and updated with a single atomic operation so a reader never sees them
    * disagree. This may be updated while there are still some in-flight data for a given data
    * source that was previously being processed.
    */
  private val stateByHost = new ConcurrentHashMap[String, StreamContext.HostState]()

  def setDataSources(host: String, ds: DataSources): Unit = {
    stateByHost.compute(
      host,
      (_, prev) =>
        // Reuse the previous state, and its already-computed expression map, if unchanged.
        if (prev != null && prev.dataSources == ds) prev
        else StreamContext.HostState(ds, interpreter.dataExprMap(ds))
    )
  }

  /**
    * Validate the input and return a new object with only the valid data sources. A diagnostic
    * message will be written to the `dsLogger` for any failures.
    */
  def validate(input: DataSources): DataSources = {
    import scala.jdk.CollectionConverters.*
    val valid = new java.util.HashSet[DataSource]()
    input.sources.asScala.foreach { ds =>
      validateDataSource(ds) match {
        case Success(v) => valid.add(v)
        case Failure(e) => dsLogger(ds, DiagnosticMessage.error(e))
      }
    }
    new DataSources(valid)
  }

  /**
    * Perform static checks to verify that the provided data source can be evaluated correctly
    * in this context.
    */
  def validateDataSource(ds: DataSource): Try[DataSource] = {
    Try {
      val uri = Uri(ds.uri)

      // Check step size is within bounds
      if (ds.step.toMillis > maxStep.toMillis) {
        throw new IllegalArgumentException(
          s"max allowed step size exceeded (${ds.step} > $maxStep)"
        )
      }

      // Check that there is a backend available for it
      val backend = findBackendForUri(uri)

      // Check that expression is parseable and perform basic static analysis of DataExprs to
      // weed out expensive queries up front
      val (exprType, exprs) = interpreter.parseQuery(uri)
      if (backend.checkForExpensiveQueries && exprType == ExprType.TIME_SERIES) {
        exprs.foreach {
          case e: StyleExpr => validateStyleExpr(e, ds)
        }
      }

      // Everything is ok
      ds
    }
  }

  private def validateStyleExpr(styleExpr: StyleExpr, ds: DataSource): Unit = {
    styleExpr.expr.dataExprs.foreach(validateDataExpr)

    // For hi-res streams, require more precise scoping that allows us to more efficiently
    // match the data and run it only where needed. This would ideally be applied everywhere,
    // but for backwards compatibility the 1m step is opted out for now.
    if (ds.step.toMillis < 60_000) {
      styleExpr.expr.dataExprs.foreach(expr => restrictsNameAndApp(expr.query))
    }
  }

  private def validateDataExpr(expr: DataExpr): Unit = {
    Query
      .dnfList(expr.query)
      .flatMap(q => Query.expandInClauses(q))
      .foreach(validateQuery)
  }

  private def validateQuery(query: Query): Unit = {
    val keys = Query.exactKeys(query) -- ignoredTagKeys
    if (keys.isEmpty) {
      val msg = s"rejected expensive query [$query], narrow the scope to a specific app or name"
      throw new IllegalArgumentException(msg)
    }
  }

  private def restrictsNameAndApp(query: Query): Unit = {
    val dnf = Query.dnfList(query)
    if (!dnf.forall(isRestricted)) {
      throw new IllegalArgumentException(
        s"rejected expensive query [$query], hi-res streams " +
          "must restrict name and nf.app with :eq or :in"
      )
    }
  }

  private def isRestricted(query: Query): Boolean = {
    isRestricted(query, Set("nf.app", "nf.cluster", "nf.asg")) && isRestricted(query, Set("name"))
  }

  private def isRestricted(query: Query, keys: Set[String]): Boolean = query match {
    case Query.And(q1, q2) => isRestricted(q1, keys) || isRestricted(q2, keys)
    case Query.Equal(k, _) => keys.contains(k)
    case Query.In(k, _)    => keys.contains(k)
    case _                 => false
  }

  /**
    * Emit an error to the sources where the number of input
    * or intermediate datapoints exceed for an expression.
    */
  /** Report exceeded limits to the data sources for `dataExpr` on a specific host. */
  def logDatapointsExceeded(host: String, timestamp: Long, dataExpr: String): Unit = {
    log(host, dataExpr, datapointsExceededMessage(timestamp, dataExpr))
  }

  /**
    * Report exceeded limits to the data sources for `dataExpr` across all hosts. Used by the
    * combined datapoint processor, where evaluation is not partitioned by host.
    */
  def logDatapointsExceeded(timestamp: Long, dataExpr: String): Unit = {
    log(dataExpr, datapointsExceededMessage(timestamp, dataExpr))
  }

  private def datapointsExceededMessage(timestamp: Long, dataExpr: String): DiagnosticMessage = {
    DiagnosticMessage.error(
      s"expression: $dataExpr exceeded the configured max input datapoints limit" +
        s" '$maxInputDatapointsPerExpression' or max intermediate" +
        s" datapoints limit '$maxIntermediateDatapointsPerExpression'" +
        s" for timestamp '$timestamp}"
    )
  }

  /**
    * Send a diagnostic message to the data sources on the given host that use a particular data
    * expression.
    */
  def log(host: String, expr: String, msg: JsonSupport): Unit = {
    val state = stateByHost.get(host)
    if (state != null) {
      state.dataExprMap.get(expr).foreach(ds => ds.foreach(s => dsLogger(s, msg)))
    }
  }

  /**
    * Send a diagnostic message to all data sources that use a particular data expression across
    * all hosts. Used where the expression is not tied to a specific host.
    */
  def log(expr: String, msg: JsonSupport): Unit = {
    stateByHost.values.forEach { state =>
      state.dataExprMap.get(expr).foreach(ds => ds.foreach(s => dsLogger(s, msg)))
    }
  }

  /**
    * Creates a set of messages for each data source on the given host that uses a given
    * expression. Scoping to the host avoids delivering data produced by one backend to a data
    * source pointed at a different backend that happens to use the same expression.
    */
  def messagesForDataSource(
    host: String,
    expr: String,
    msg: JsonSupport
  ): List[Evaluator.MessageEnvelope] = {
    val state = stateByHost.get(host)
    val dataSources = if (state == null) None else state.dataExprMap.get(expr)
    dataSources match {
      case Some(ds) =>
        ds.map(s => new Evaluator.MessageEnvelope(s.id, msg))
      case None =>
        // No active data source maps to this expression, so the message would be dropped.
        // Track it and log the details to make this observable rather than silent.
        unmatchedMessages.increment()
        logger.debug(s"dropping message, no data source for expression [$expr] on host [$host]")
        Nil
    }
  }

  /**
    * Returns a simple http client flow that will log the request using the provided name.
    */
  def httpClient(name: String): SimpleClient = {
    PekkoHttpClient.create(name, materializer.system).simpleFlow()
  }

  def monitorFlow[T](phase: String): Flow[T, T, NotUsed] = {
    StreamOps.monitorFlow(registry, phase)
  }
}

private[stream] object StreamContext {

  /** Per-host data sources and the expression map derived from them, updated together. */
  case class HostState(dataSources: DataSources, dataExprMap: Map[String, List[DataSource]])

  sealed trait Backend {

    def source: Source[ByteString, Future[IOResult]]

    def checkForExpensiveQueries: Boolean
  }

  case class FileBackend(file: Path) extends Backend {

    def source: Source[ByteString, Future[IOResult]] = {
      FileIO.fromPath(file).via(EvaluationFlows.sseFraming)
    }

    def checkForExpensiveQueries: Boolean = true
  }

  case class ResourceBackend(resource: String) extends Backend {

    def source: Source[ByteString, Future[IOResult]] = {
      StreamConverters
        .fromInputStream(() => Streams.resource(resource))
        .via(EvaluationFlows.sseFraming)
    }

    def checkForExpensiveQueries: Boolean = true
  }

  case class SyntheticBackend(interpreter: ExprInterpreter, uri: Uri) extends Backend {

    def source: Source[ByteString, Future[IOResult]] = {
      SyntheticDataSource(interpreter, uri)
    }

    def checkForExpensiveQueries: Boolean = true
  }

  case class EddaBackend(
    host: String,
    eddaUri: String,
    checkForExpensiveQueries: Boolean = true
  ) extends Backend {

    def source: Source[ByteString, Future[IOResult]] = {
      throw new UnsupportedOperationException("only supported for file and classpath URIs")
    }
  }
}
