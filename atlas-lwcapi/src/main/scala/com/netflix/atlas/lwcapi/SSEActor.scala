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
package com.netflix.atlas.lwcapi

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}
import com.netflix.iep.NetflixEnvironment
import com.netflix.spectator.api.Registry

import scala.concurrent.ExecutionContext.Implicits.global
import spray.can.Http
import spray.http._

class SSEActor(client: ActorRef,
               sseId: String,
               name: String,
               sm: SubscriptionManager,
               registry: Registry)
  extends Actor with ActorLogging {
  import SSEActor._
  import StreamApi._

  private val sseMediaType = MediaType.custom("text", "event-stream", compressible = true, binary = false)

  private val connectsId = registry.createId("atlas.lwcapi.sse.connectCount")
  private val messagesId = registry.createId("atlas.lwcapi.sse.messageCount")
  private val messageBytesId = registry.createId("atlas.lwcapi.sse.messageBytesCount")
  private val sendsId = registry.createId("atlas.lwcapi.sse.httpSendsCount")
  private val sendBytesId = registry.createId("atlas.lwcapi.sse.httpSendBytesCount")
  private val droppedId = registry.createId("atlas.lwcapi.sse.droppedMessageCount")

  private val sseCountId = registry.createId("atlas.lwcapi.streams").withTag("streamType", "sse")
  private val sseCount = registry.gauge(sseCountId, new AtomicInteger(1))

  private var outstandingByteCount: Long = 0
  private val maxOutstandingBytes: Long = 10000000 // 10 megabytes max outstanding request size per SSE stream
  private var droppedMessageCount: Long = 0 // in messages
  private val maxBufferLength: Long = 100000 // 100k allowed to buffer before we send it.

  private val instanceId = NetflixEnvironment.instanceId()

  private val tickTime = 100.microseconds
  private var tickCount = 0
  private val statsTime = 300 // in 100ms increments.
  private val helloMessage = SSEHello(sseId, instanceId, GlobalUUID.get)
  private val messageBuffer = new StringBuilder

  // The first response sets the content-type.  Note that we must return some data, since
  // even an empty string results in no content-type being set.
  val entity = HttpEntity(sseMediaType, "info: Connected {}\r\n\r\n")
  client ! ChunkedResponseStart(HttpResponse(StatusCodes.OK, entity = entity)).withAck(Ack(0))
  registry.counter(connectsId.withTag("streamId", sseId)).increment()

  var needsUnregister = true
  sm.register(sseId, self, name)
  send(helloMessage)
  send(SSEStatistics(0))

  var ticker: Cancellable = context.system.scheduler.schedule(tickTime, tickTime) {
    self ! Tick
  }

  def receive = {
    case Ack(length) =>
      outstandingByteCount -= length
    case Tick =>
      tickCount += 1
      if (tickCount >= statsTime) {
        val msg = SSEStatistics(droppedMessageCount)
        send(msg, force = true)
        tickCount = 0
      }
      flushBuffer()
    case msg: SSEShutdown =>
      ticker.cancel()
      send(msg)
      flushBuffer()
      client ! Http.Close
      unregister()
      log.info(s"Closing SSE stream: ${msg.reason}")
    case msg: SSEMessage =>
      send(msg)
    case closed: Http.ConnectionClosed =>
      ticker.cancel()
      log.info(s"SSE Stream closed: $closed")
      context.stop(self)
  }

  private def flushBuffer(): Unit = {
    if (messageBuffer.nonEmpty) {
      val bufferLength = messageBuffer.length
      registry.counter(sendBytesId).increment(bufferLength)
      registry.counter(sendsId).increment()
      client ! MessageChunk(messageBuffer.result).withAck(Ack(bufferLength))
      messageBuffer.clear
    }
  }

  private def queueToBuffer(msg: SSEMessage): Unit = {
    val part = msg.toSSE + "\r\n\r\n"
    messageBuffer.append(part)
    // accounting happens prior to actual send
    outstandingByteCount += part.length
    registry.counter(messagesId.withTag("action", msg.getWhat)).increment()
    registry.counter(messageBytesId.withTag("action", msg.getWhat)).increment(part.length)
    if (messageBuffer.length >= maxBufferLength)
      flushBuffer()
  }

  private def send(msg: SSEMessage, force: Boolean = false): Unit = {
    if (force) {
      queueToBuffer(msg)
      flushBuffer()
    } else {
      if (outstandingByteCount < maxOutstandingBytes) {
        queueToBuffer(msg)
      } else {
        droppedMessageCount += 1
        registry.counter(droppedId.withTag("action", msg.getWhat)).increment()
      }
    }
  }

  private def unregister() = {
    if (needsUnregister) {
      needsUnregister = false
      sm.unregister(sseId)
      sseCount.decrementAndGet()
    }
  }

  override def postStop() = {
    unregister()
    ticker.cancel()
    super.postStop()
  }

  case class Ack(length: Long)
}

object SSEActor {
  case object Tick
}
