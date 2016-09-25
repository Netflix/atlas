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

import scala.concurrent.duration._
import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}

import scala.concurrent.ExecutionContext.Implicits.global
import spray.can.Http
import spray.http._

class SSEActor(client: ActorRef, sseId: String, name: String, sm: SubscriptionManager)
  extends Actor with ActorLogging {
  import SSEActor._
  import StreamApi._

  private var outstandingCount = 0
  private val maxOutstanding = 100
  private var droppedCount = 0

  private val instanceId = sys.env.getOrElse("EC2_INSTANCE_ID", "unknown")

  private val tickTime = 30.seconds
  private val tickMessage = SSEHeartbeat()
  private val helloMessage = SSEHello(sseId, instanceId, GlobalUUID.get)

  client ! ChunkedResponseStart(HttpResponse(StatusCodes.OK)).withAck(Ack())
  outstandingCount += 1

  var unregister = true
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
      unregister = msg.shouldUnregister
      if (unregister)
        sm.unregister(sseId)
      log.info(s"Closing SSE stream: ${msg.reason}, shouldUnregister: $unregister")
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
    } else {
      droppedCount += 1
    }
  }

  override def postStop() = {
    if (unregister)
      sm.unregister(sseId)
    ticker.cancel()
    super.postStop()
  }
}

object SSEActor {
  case class Ack()
  case class Tick()
}
