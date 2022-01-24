/*
 * Copyright 2014-2022 Netflix, Inc.
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

import akka.NotUsed
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.Uri
import akka.stream.IOResult
import akka.stream.Materializer
import akka.stream.scaladsl.FileIO
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.StreamConverters
import akka.util.ByteString
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.StreamOps
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

private[stream] class StreamContext(
  rootConfig: Config,
  val client: Client,
  val materializer: Materializer,
  val registry: Registry = new NoopRegistry,
  val dsLogger: DataSourceLogger = (_, _) => ()
) {

  import StreamContext._

  val id: String = UUID.randomUUID().toString

  private val config = rootConfig.getConfig("atlas.eval.stream")

  private val backends = {
    import scala.jdk.CollectionConverters._
    config.getConfigList("backends").asScala.toList.map { cfg =>
      EurekaBackend(
        cfg.getString("host"),
        cfg.getString("eureka-uri"),
        cfg.getString("instance-uri")
      )
    }
  }

  private val ignoredTagKeys = {
    import scala.jdk.CollectionConverters._
    config.getStringList("ignored-tag-keys").asScala.toSet
  }

  def numBuffers: Int = config.getInt("num-buffers")

  // Maximum number of raw input data points for a data expr.
  def maxInputDatapointsPerExpression: Int = config.getInt("limits.max-input-datapoints")

  // Maximum number of datapoints resulting from a group by for a data expr.
  def maxIntermediateDatapointsPerExpression: Int =
    config.getInt("limits.max-intermediate-datapoints")

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

  def findEurekaBackendForUri(uri: Uri): EurekaBackend = {
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
  @volatile private var dataExprMap: Map[DataExpr, List[DataSource]] = Map.empty

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
    import scala.jdk.CollectionConverters._
    val valid = new java.util.HashSet[DataSource]()
    input.getSources.asScala.foreach { ds =>
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
      val uri = Uri(ds.getUri)

      // Check that expression is parseable and perform basic static analysis of DataExprs to
      // weed out expensive queries up front
      val results = interpreter.eval(uri)
      results.foreach(_.expr.dataExprs.foreach(validateDataExpr))

      // Check that there is a backend available for it
      findBackendForUri(uri)

      // Everything is ok
      ds
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

  /**
    * Emit an error to the sources where the number of input
    * or intermediate datapoints exceed for an expression.
    */
  def logDatapointsExceeded(timestamp: Long, dataExpr: DataExpr) = {
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
  def log(expr: DataExpr, msg: DiagnosticMessage): Unit = {
    dataExprMap.get(expr).foreach { ds =>
      ds.foreach(s => dsLogger(s, msg))
    }
  }

  /**
    * Returns a simple http client flow that will log the request using the provide name.
    */
  def httpClient(name: String): SimpleClient = {
    Flow[HttpRequest]
      .map(r => r -> AccessLogger.newClientLogger(name, r))
      .via(client)
      .map {
        case (response, log) =>
          log.complete(response)
          response
      }
  }

  def monitorFlow[T](phase: String): Flow[T, T, NotUsed] = {
    StreamOps.monitorFlow(registry, phase)
  }
}

private[stream] object StreamContext {

  sealed trait Backend {
    def source: Source[ByteString, Future[IOResult]]
  }

  case class FileBackend(file: Path) extends Backend {

    def source: Source[ByteString, Future[IOResult]] = {
      FileIO.fromPath(file).via(EvaluationFlows.sseFraming)
    }
  }

  case class ResourceBackend(resource: String) extends Backend {

    def source: Source[ByteString, Future[IOResult]] = {
      StreamConverters
        .fromInputStream(() => Streams.resource(resource))
        .via(EvaluationFlows.sseFraming)
    }
  }

  case class SyntheticBackend(interpreter: ExprInterpreter, uri: Uri) extends Backend {

    def source: Source[ByteString, Future[IOResult]] = {
      SyntheticDataSource(interpreter, uri)
    }
  }

  case class EurekaBackend(host: String, eurekaUri: String, instanceUri: String) extends Backend {

    def source: Source[ByteString, Future[IOResult]] = {
      throw new UnsupportedOperationException("only supported for file and classpath URIs")
    }
  }
}
