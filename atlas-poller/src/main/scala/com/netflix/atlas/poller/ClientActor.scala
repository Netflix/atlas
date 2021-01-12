/*
 * Copyright 2014-2021 Netflix, Inc.
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
package com.netflix.atlas.poller

import akka.actor.Actor
import akka.actor.ActorRef
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.headers._
import akka.stream.Materializer
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.akka.CustomMediaTypes
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.json.Json
import com.netflix.atlas.poller.Messages.MetricsPayload
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Registry
import com.netflix.spectator.impl.AsciiSet
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

/**
  * Sink for the poller data that publishes the metrics to Atlas. The actor expects
  * [[Messages.MetricsPayload]] messages and does not send a response. Failures will
  * be logged and reflected in the `atlas.client.dropped` counter as well as standard
  * client access logging.
  */
class ClientActor(registry: Registry, config: Config, implicit val materializer: Materializer)
    extends Actor {

  private implicit val xc = scala.concurrent.ExecutionContext.global

  private val logger = LoggerFactory.getLogger(getClass)

  private val uri = config.getString("uri")
  private val batchSize = config.getInt("batch-size")

  private val shouldSendAck = config.getBoolean("send-ack")

  private val validTagChars = AsciiSet.fromPattern(config.getString("valid-tag-characters"))

  private val validTagValueChars = {
    import scala.jdk.CollectionConverters._
    config
      .getConfigList("valid-tag-value-characters")
      .asScala
      .map { cfg =>
        cfg.getString("key") -> AsciiSet.fromPattern(cfg.getString("value"))
      }
      .toMap
  }

  private val datapointsSent = registry.counter("atlas.client.sent")
  private val datapointsDropped = registry.createId("atlas.client.dropped")

  def receive: Receive = {
    case MetricsPayload(_, ms) =>
      val responder = sender()
      datapointsSent.increment(ms.size)
      ms.grouped(batchSize).foreach { batch =>
        val msg = MetricsPayload(metrics = batch.map(fixTags))
        post(msg).onComplete {
          case Success(response) => handleResponse(responder, response, batch.size)
          case Failure(t)        => handleFailure(responder, t, batch.size)
        }
      }
  }

  private def fixTags(d: Datapoint): Datapoint = {
    val tags = d.tags.map {
      case (k, v) =>
        val nk = validTagChars.replaceNonMembers(k, '_')
        val nv = validTagValueChars.getOrElse(nk, validTagChars).replaceNonMembers(v, '_')
        nk -> nv
    }
    d.copy(tags = tags)
  }

  private def post(data: MetricsPayload): Future[HttpResponse] = {
    post(Json.smileEncode(data))
  }

  /**
    * Encode the data and start post to atlas. Method is protected to allow for
    * easier testing.
    */
  protected def post(data: Array[Byte]): Future[HttpResponse] = {
    val request = HttpRequest(
      HttpMethods.POST,
      uri = uri,
      headers = ClientActor.headers,
      entity = HttpEntity(CustomMediaTypes.`application/x-jackson-smile`, data)
    )
    val accessLogger = AccessLogger.newClientLogger("atlas_publish", request)
    Http()(context.system).singleRequest(request).andThen { case t => accessLogger.complete(t) }
  }

  private def handleResponse(responder: ActorRef, response: HttpResponse, size: Int): Unit = {
    response.status.intValue match {
      case 200 => // All is well
        response.discardEntityBytes()
      case 202 => // Partial failure
        val id = datapointsDropped.withTag("id", "PartialFailure")
        incrementFailureCount(id, response, size)
      case 400 => // Bad message, all data dropped
        val id = datapointsDropped.withTag("id", "CompleteFailure")
        incrementFailureCount(id, response, size)
      case v => // Unexpected, assume all dropped
        response.discardEntityBytes()
        val id = datapointsDropped.withTag("id", s"Status_$v")
        registry.counter(id).increment(size)
    }
    if (shouldSendAck) responder ! Messages.Ack
  }

  private def incrementFailureCount(id: Id, response: HttpResponse, size: Int): Unit = {
    response.entity.dataBytes.runReduce(_ ++ _).onComplete {
      case Success(bs) =>
        val msg = Json.decode[Messages.FailureResponse](bs.toArray)
        msg.message.headOption.foreach { reason =>
          logger.warn("failed to validate some datapoints, first reason: {}", reason)
        }
        registry.counter(id).increment(msg.errorCount)
      case Failure(_) =>
        registry.counter(id).increment(size)
    }
  }

  private def handleFailure(responder: ActorRef, t: Throwable, size: Int): Unit = {
    val id = datapointsDropped.withTag("id", t.getClass.getSimpleName)
    registry.counter(id).increment(size)
    if (shouldSendAck) responder ! Messages.Ack
  }
}

object ClientActor {
  private val gzip = HttpEncodingRange(HttpEncodings.gzip)
  private val headers = List(`Accept-Encoding`(gzip), Accept(MediaTypes.`application/json`))
}
