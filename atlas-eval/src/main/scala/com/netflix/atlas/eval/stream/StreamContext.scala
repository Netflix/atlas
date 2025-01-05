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
package com.netflix.atlas.eval.stream

import java.nio.file.Path
import java.nio.file.Paths
import java.util.UUID
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
import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.pekko.DiagnosticMessage
import com.netflix.atlas.pekko.PekkoHttpClient
import com.netflix.atlas.pekko.StreamOps
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

private[stream] class StreamContext(
  rootConfig: Config,
  val materializer: Materializer,
  val registry: Registry = new NoopRegistry,
  val dsLogger: DataSourceLogger = DataSourceLogger.Noop
) {

  import StreamContext.*

  val id: String = UUID.randomUUID().toString

  private val config = rootConfig.getConfig("atlas.eval.stream")

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
    * Current set of data sources being processed by the stream. This may be updated while
    * there are still some in-flight data for a given data source that was previously being
    * processed.
    */
  @volatile private var dataSources: DataSources = DataSources.empty()

  /** Map of DataExpr to data sources for being able to quickly log information. */
  @volatile private var dataExprMap: Map[String, List[DataSource]] = Map.empty

  def setDataSources(ds: DataSources): Unit = {
    if (dataSources != ds) {
      dataSources = ds
      dataExprMap = interpreter.dataExprMap(ds)
    }
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
    * Perform static checks to verify that the provide data source can be evaluated correctly
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
    // but for backwards compatiblity the 1m step is opted out for now.
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
  def logDatapointsExceeded(timestamp: Long, dataExpr: String): Unit = {
    val diagnosticMessage = DiagnosticMessage.error(
      s"expression: $dataExpr exceeded the configured max input datapoints limit" +
        s" '$maxInputDatapointsPerExpression' or max intermediate" +
        s" datapoints limit '$maxIntermediateDatapointsPerExpression'" +
        s" for timestamp '$timestamp}"
    )
    log(dataExpr, diagnosticMessage)
  }

  /**
    * Send a diagnostic message to all data sources that use a particular data expression.
    */
  def log(expr: String, msg: JsonSupport): Unit = {
    dataExprMap.get(expr).foreach { ds =>
      ds.foreach(s => dsLogger(s, msg))
    }
  }

  /**
    * Creates a set of messages for each data source that uses a given expression.
    */
  def messagesForDataSource(expr: String, msg: JsonSupport): List[Evaluator.MessageEnvelope] = {
    dataExprMap.get(expr).toList.flatMap { ds =>
      ds.map(s => new Evaluator.MessageEnvelope(s.id, msg))
    }
  }

  /**
    * Returns a simple http client flow that will log the request using the provide name.
    */
  def httpClient(name: String): SimpleClient = {
    PekkoHttpClient.create(name, materializer.system).simpleFlow()
  }

  def monitorFlow[T](phase: String): Flow[T, T, NotUsed] = {
    StreamOps.monitorFlow(registry, phase)
  }
}

private[stream] object StreamContext {

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
