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
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.json.Json
import spray.can.Http
import spray.http._

class SSEActor(client: ActorRef, sseId: String, sm: SubscriptionManager) extends Actor with ActorLogging {
  import com.netflix.atlas.lwcapi.StreamAPI._

  private var outstandingCount = 0
  private val maxOutstanding = 100
  private var droppedCount = 0

  private val tickTime = 30.seconds
  private val tickMessage = SSEHeartbeat()
  private val helloMessage = SSEHello(sseId)

  client ! ChunkedResponseStart(HttpResponse(StatusCodes.OK)).withAck(Ack())
  outstandingCount += 1
  client ! send(helloMessage)

  var ticker: Cancellable = context.system.scheduler.scheduleOnce(tickTime) { self ! Tick() }

  def receive = {
    case Ack() =>
      outstandingCount -= 1
    case Tick() =>
      if (outstandingCount == 0) send(tickMessage)
      ticker = context.system.scheduler.scheduleOnce(tickTime) { self ! Tick() }
    case msg: SSEShutdown =>
      send(msg)
      client ! Http.Close
      ticker.cancel()
      sm.unsubscribeAll(sseId)
      log.info(s"Closing SSE stream: ${msg.reason}")
    case msg: SSEMessage =>
      send(msg)
    case closed: Http.ConnectionClosed =>
      ticker.cancel()
      log.info(s"SSE Stream closed: $closed")
      context.stop(self)
    case _ =>
      DiagnosticMessage.sendError(sender(), StatusCodes.BadRequest, "unknown payload")
  }

  private def send(msg: SSEMessage, force: Boolean = false): Unit = {
    if (force || outstandingCount < maxOutstanding) {
      val json = msg.toJson + "\r\n\r\n"
      client ! MessageChunk(json).withAck(Ack())
      outstandingCount += 1
    } else {
      droppedCount += 1
    }
  }

  override def postStop() = {
    sm.unsubscribeAll(sseId)
    ticker.cancel()
    super.postStop()
  }

  case class Ack()
  case class Tick()
  case class ShutdownMessage()
}
