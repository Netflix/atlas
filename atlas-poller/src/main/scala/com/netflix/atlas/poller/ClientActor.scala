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
package com.netflix.atlas.poller

import akka.actor.Actor
import akka.actor.ActorRef
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.akka.CustomMediaTypes
import com.netflix.atlas.json.Json
import com.netflix.atlas.poller.Messages.MetricsPayload
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import spray.client.pipelining._
import spray.http.HttpEncodingRange
import spray.http.HttpEncodings
import spray.http.HttpEntity
import spray.http.HttpHeaders
import spray.http.HttpMethods
import spray.http.HttpRequest
import spray.http.HttpResponse
import spray.http.MediaTypes
import spray.httpx.encoding.Gzip

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

/**
  * Sink for the poller data that publishes the metrics to Atlas. The actor expects
  * [[Messages.MetricsPayload]] messages and does not send a response. Failures will
  * be logged and reflected in the `atlas.client.dropped` counter as well as standard
  * client access logging.
  */
class ClientActor(registry: Registry, config: Config) extends Actor {

  private implicit val xc = scala.concurrent.ExecutionContext.global

  private val logger = LoggerFactory.getLogger(getClass)

  private val pipeline: SendReceive = sendReceive ~> decode(Gzip)

  private val uri = config.getString("uri")
  private val batchSize = config.getInt("batch-size")

  private val shouldSendAck = config.getBoolean("send-ack")

  private val datapointsSent = registry.counter("atlas.client.sent")
  private val datapointsDropped = registry.createId("atlas.client.dropped")

  def receive: Receive = {
    case MetricsPayload(_, ms) =>
      val responder = sender()
      datapointsSent.increment(ms.size)
      ms.grouped(batchSize).foreach { batch =>
        val msg = MetricsPayload(metrics = batch)
        post(msg).onComplete {
          case Success(response) => handleResponse(responder, response, batch.size)
          case Failure(t)        => handleFailure(responder, t, batch.size)
        }
      }
  }

  private def post(data: MetricsPayload): Future[HttpResponse] = {
    post(Json.smileEncode(data))
  }

  /**
    * Encode the data and start post to atlas. Method is protected to allow for
    * easier testing.
    */
  protected def post(data: Array[Byte]): Future[HttpResponse] = {
    val request = HttpRequest(HttpMethods.POST,
      uri = uri,
      headers = ClientActor.headers,
      entity = HttpEntity(CustomMediaTypes.`application/x-jackson-smile`, data))
    val accessLogger = AccessLogger.newClientLogger("atlas_publish", request)
    pipeline(request).andThen { case t => accessLogger.complete(t) }
  }

  private def handleResponse(responder: ActorRef, response: HttpResponse, size: Int): Unit = {
    response.status.intValue match {
      case 200 => // All is well
      case 202 => // Partial failure
        val id = datapointsDropped.withTag("id", "PartialFailure")
        registry.counter(id).increment(determineFailureCount(response, size))
      case 400 => // Bad message, all data dropped
        val id = datapointsDropped.withTag("id", "CompleteFailure")
        registry.counter(id).increment(determineFailureCount(response, size))
      case v   => // Unexpected, assume all dropped
        val id = datapointsDropped.withTag("id", s"Status_$v")
        registry.counter(id).increment(size)
    }
    if (shouldSendAck) responder ! Messages.Ack
  }

  private def determineFailureCount(response: HttpResponse, size: Int): Int = {
    try {
      val msg = Json.decode[Messages.FailureResponse](response.entity.data.toByteArray)
      msg.message.headOption.foreach { reason =>
        logger.warn("failed to validate some datapoints, first reason: {}", reason)
      }
      msg.errorCount
    } catch {
      case _: Exception => size
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
  private val headers = List(
    HttpHeaders.`Accept-Encoding`(Seq(gzip)),
    HttpHeaders.Accept(MediaTypes.`application/json`)
  )
}
