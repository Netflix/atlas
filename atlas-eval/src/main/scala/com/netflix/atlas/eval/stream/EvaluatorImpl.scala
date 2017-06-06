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
package com.netflix.atlas.eval.stream

import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.FileIO
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.StreamConverters
import akka.util.ByteString
import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.json.JsonSupport
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import org.reactivestreams.Processor
import org.reactivestreams.Publisher

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

/**
  * Internal implementation details for [[Evaluator]]. Anything needing a stable API should
  * be using that class directly.
  */
private[stream] abstract class EvaluatorImpl(
    config: Config,
    registry: Registry,
    implicit val system: ActorSystem) {

  import EvaluatorImpl._

  private implicit val materializer = ActorMaterializer()

  private val backends = {
    import scala.collection.JavaConverters._
    config.getConfigList("atlas.eval.stream.backends").asScala.toList.map { cfg =>
      EurekaBackend(
        cfg.getString("host"),
        cfg.getString("eureka-uri"),
        cfg.getString("instance-uri"))
    }
  }

  private def findBackendForUri(uri: Uri): Backend = {
    if (uri.isRelative || uri.scheme == "file")
      FileBackend(Paths.get(uri.path.toString()))
    else if (uri.scheme == "resource")
      ResourceBackend(uri.path.toString().substring(1))
    else
      findEurekaBackendForUri(uri)
  }

  private def findEurekaBackendForUri(uri: Uri): Backend = {
    val id = UUID.randomUUID().toString
    val expr = uri.query().get("q").get
    val path = s"/lwc/api/v1/stream/$id?expression=$expr"

    val host = uri.authority.host.address()
    backends.find(_.host == host) match {
      case Some(backend) => backend.copy(instanceUri = backend.instanceUri + path)
      case None          => throw new NoSuchElementException(host)
    }
  }

  protected def writeInputToFileImpl(uri: String, file: Path, duration: Duration): Unit = {
    val d = FiniteDuration(duration.toMillis, TimeUnit.MILLISECONDS)
    writeInputToFileImpl(Uri(uri), file, d)
  }

  protected def writeInputToFileImpl(uri: Uri, file: Path, duration: FiniteDuration): Unit = {
    val backend = findBackendForUri(uri)

    // Explicit type needed in 2.5.2, but not 2.5.0. Likely related to:
    // https://github.com/akka/akka/issues/22666
    val options = Set[OpenOption](
      StandardOpenOption.WRITE,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING)

    // Carriage returns can cause a lot of confusion when looking at the file. Clean it up
    // to be more unix friendly before writing to the file.
    val sink = Flow[ByteString]
      .map(_.filterNot(_ == '\r'))
      .filterNot(_.isEmpty)
      .map(_ ++ ByteString("\n\n"))
      .toMat(FileIO.toPath(file, options))(Keep.right)

    val ref = backend.run(sink)
    try {
      // If it is coming from a finite source like a file it will likely complete
      // before the timeout
      val result = Await.result(ref.value, duration)
      if (!result.wasSuccessful) throw result.getError
    } catch {
      // For eureka sources they will go on forever, when the requested timeout
      // expires shutdown the stream and notify the user of any problems
      case e: TimeoutException =>
        ref.killSwitch.shutdown()
        val result = Await.result(ref.value, duration)
        if (!result.wasSuccessful) throw result.getError
    }
  }

  protected def createPublisherImpl(uri: String): Publisher[JsonSupport] = {
    createPublisherImpl(Uri(uri))
  }

  protected def createPublisherImpl(uri: Uri): Publisher[JsonSupport] = {
    val backend = findBackendForUri(uri)

    val expr = eval(uri.query().get("q").get).head
    val sink = EvaluationFlows.lwcEval(expr, 60000)
      .toMat(Sink.asPublisher(true))(Keep.right)

    backend.run(sink).value
  }

  protected def createStreamsProcessorImpl(): Processor[Evaluator.DataSources, Evaluator.MessageEnvelope] = {
    Flow[Evaluator.DataSources]
      .via(new DataSourceManager(ds => Source.fromPublisher(createPublisherImpl(ds.getUri))))
      .flatMapMerge(Int.MaxValue, v => v)
      .toProcessor
      .run()
  }
}

private[stream] object EvaluatorImpl {

  private val interpreter = Interpreter(StyleVocabulary.allWords)

  def eval(expr: String): List[StyleExpr] = {
    interpreter.execute(expr).stack.map {
      case ModelExtractors.PresentationType(t) => t
    }
  }

  sealed trait Backend {
    def run[T](sink: Sink[ByteString, T])(implicit sys: ActorSystem, mat: ActorMaterializer): StreamRef[T]
  }

  case class FileBackend(file: Path) extends Backend {
    def run[T](sink: Sink[ByteString, T])(implicit sys: ActorSystem, mat: ActorMaterializer): StreamRef[T] = {
      val source = FileIO.fromPath(file).via(EvaluationFlows.sseFraming)
      EvaluationFlows.run(source, sink)
    }
  }

  case class ResourceBackend(resource: String) extends Backend {
    def run[T](sink: Sink[ByteString, T])(implicit sys: ActorSystem, mat: ActorMaterializer): StreamRef[T] = {
      val source = StreamConverters
        .fromInputStream(() => Streams.resource(resource))
        .via(EvaluationFlows.sseFraming)
      EvaluationFlows.run(source, sink)
    }
  }

  case class EurekaBackend(host: String, eurekaUri: String, instanceUri: String) extends Backend {
    def run[T](sink: Sink[ByteString, T])(implicit sys: ActorSystem, mat: ActorMaterializer): StreamRef[T] = {
      val client = Http().superPool[NotUsed]()
      val src = EurekaSource(eurekaUri, instanceUri, client)
      EvaluationFlows.run(src, sink)
    }
  }
}
