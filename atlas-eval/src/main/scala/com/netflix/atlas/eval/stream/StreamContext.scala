/*
 * Copyright 2014-2018 Netflix, Inc.
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
import akka.stream.ActorMaterializer
import akka.stream.IOResult
import akka.stream.scaladsl.FileIO
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.StreamConverters
import akka.util.ByteString
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config

import scala.concurrent.Future

private[stream] class StreamContext(
  rootConfig: Config,
  val client: Client,
  val materializer: ActorMaterializer,
  val registry: Registry = new NoopRegistry,
  val dsLogger: DataSourceLogger = (_, _) => ()
) {

  import StreamContext._

  val id: String = UUID.randomUUID().toString

  private val config = rootConfig.getConfig("atlas.eval.stream")

  private val backends = {
    import scala.collection.JavaConverters._
    config.getConfigList("backends").asScala.toList.map { cfg =>
      EurekaBackend(
        cfg.getString("host"),
        cfg.getString("eureka-uri"),
        cfg.getString("instance-uri")
      )
    }
  }

  def numBuffers: Int = config.getInt("num-buffers")

  val interpreter = new ExprInterpreter(rootConfig)

  def findBackendForUri(uri: Uri): Backend = {
    if (uri.isRelative || uri.scheme == "file")
      FileBackend(Paths.get(uri.path.toString()))
    else if (uri.scheme == "resource")
      ResourceBackend(uri.path.toString().substring(1))
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

  def validate(input: DataSources): DataSources = {
    import scala.collection.JavaConverters._
    val valid = input.getSources.asScala.flatMap(validateDataSource).asJava
    new DataSources(valid)
  }

  def validateDataSource(ds: DataSource): Option[DataSource] = {
    try {
      val uri = Uri(ds.getUri)

      // Check that expression is parseable
      interpreter.eval(uri)

      // Check that there is a backend available for it
      findBackendForUri(uri)

      // Everything is ok
      Some(ds)
    } catch {
      case e: Exception =>
        dsLogger(ds, DiagnosticMessage.error(e))
        None
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

  def countEvents[T](phase: String): Flow[T, T, NotUsed] = {
    val counter = registry.counter("atlas.eval.numEvents", "id", phase)
    Flow[T].map { value =>
      counter.increment()
      value
    }
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

  case class EurekaBackend(host: String, eurekaUri: String, instanceUri: String) extends Backend {

    def source: Source[ByteString, Future[IOResult]] = {
      throw new UnsupportedOperationException("only supported for file and classpath URIs")
    }
  }
}
