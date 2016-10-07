/*
 * Copyright 2014-2016 Netflix, Inc.
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

  private val connectsId = registry.createId("atlas.lwcapi.sse.connectCount")
  private val messagesId = registry.createId("atlas.lwcapi.sse.messageCount")
  private val droppedId = registry.createId("atlas.lwcapi.sse.droppedCount")

  private val sseCountId = registry.createId("atlas.lwcapi.streams").withTag("streamType", "sse")
  private val sseCount = registry.gauge(sseCountId, new AtomicInteger(1))

  private var outstandingCount = 0
  private val maxOutstanding = 100
  private var droppedCount = 0

  private val instanceId = sys.env.getOrElse("EC2_INSTANCE_ID", "unknown")

  private val tickTime = 30.seconds
  private val tickMessage = SSEHeartbeat()
  private val helloMessage = SSEHello(sseId, instanceId, GlobalUUID.get)

  client ! ChunkedResponseStart(HttpResponse(StatusCodes.OK)).withAck(Ack())
  outstandingCount += 1
  registry.counter(connectsId.withTag("streamId", sseId)).increment()

  var needsUnregister = true
  sm.register(sseId, self, name)
  send(helloMessage)

  var ticker: Cancellable = context.system.scheduler.scheduleOnce(tickTime) {
    self ! Tick()
  }

  def receive = {
    case Ack() =>
      outstandingCount -= 1
    case Tick() =>
      if (outstandingCount == 0) send(tickMessage)
      ticker = context.system.scheduler.scheduleOnce(tickTime) {
        self ! Tick()
      }
    case msg: SSEShutdown =>
      send(msg)
      client ! Http.Close
      ticker.cancel()
      unregister()
      log.info(s"Closing SSE stream: ${msg.reason}")
    case msg: SSEMessage =>
      send(msg)
    case closed: Http.ConnectionClosed =>
      ticker.cancel()
      log.info(s"SSE Stream closed: $closed")
      context.stop(self)
  }

  private def send(msg: SSEMessage): Unit = {
    if (outstandingCount < maxOutstanding) {
      val json = msg.toSSE + "\r\n\r\n"
      client ! MessageChunk(json).withAck(Ack())
      outstandingCount += 1
      registry.counter(messagesId.withTag("action", msg.getWhat).withTag("streamId", sseId)).increment()
    } else {
      droppedCount += 1
      registry.counter(droppedId.withTag("action", msg.getWhat).withTag("streamId", sseId)).increment()
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
}

object SSEActor {
  case class Ack()
  case class Tick()
}
