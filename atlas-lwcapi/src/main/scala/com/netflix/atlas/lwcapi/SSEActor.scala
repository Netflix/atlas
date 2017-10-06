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

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorLogging
import akka.actor.Cancellable
import akka.http.scaladsl.model.HttpEntity.ChunkStreamPart
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage._
import com.netflix.iep.NetflixEnvironment
import com.netflix.spectator.api.Registry

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class SSEActor(
  sseId: String,
  name: String,
  sm: ActorSubscriptionManager,
  subs: Map[ExpressionMetadata, List[Subscription]],
  registry: Registry
) extends ActorPublisher[ChunkStreamPart]
    with ActorLogging {

  import SSEActor._
  import StreamApi._

  private val connectsId = registry.createId("atlas.lwcapi.sse.connectCount")
  private val messagesId = registry.createId("atlas.lwcapi.sse.messageCount")
  private val messageBytesId = registry.createId("atlas.lwcapi.sse.messageBytesCount")
  private val sendsId = registry.createId("atlas.lwcapi.sse.httpSendsCount")
  private val sendBytesId = registry.createId("atlas.lwcapi.sse.httpSendBytesCount")
  private val droppedId = registry.createId("atlas.lwcapi.sse.droppedMessageCount")

  private val sseCountId = registry.createId("atlas.lwcapi.streams").withTag("streamType", "sse")
  private val sseCount = registry.gauge(sseCountId, new AtomicInteger(1))

  private var droppedMessageCount: Long = 0 // in messages

  private val instanceId = NetflixEnvironment.instanceId()

  private val diagnosticMessages = new ArrayBlockingQueue[String](10)
  private val messages = new ArrayBlockingQueue[String](10000)

  registry.counter(connectsId.withTag("streamId", sseId)).increment()

  private var needsUnregister = true
  sm.register(sseId, self)
  enqueue(SSEHello(sseId, instanceId), diagnosticMessages)
  enqueue(SSEStatistics(0), diagnosticMessages)
  subs.foreach {
    case (expr, subscriptions) =>
      subscriptions.foreach { s =>
        sm.subscribe(sseId, s)
      }
      enqueue(SSESubscribe(expr.expression, subscriptions.map(_.metadata)), diagnosticMessages)
  }

  private var shutdown = false

  private val tickTime = 10.seconds
  private val ticker: Cancellable = context.system.scheduler.schedule(tickTime, tickTime) {
    self ! Tick
  }

  def receive: Receive = {
    case Request(_) =>
      writeChunks()
    case Cancel =>
      log.info(s"SSE Stream closed")
      context.stop(self)
    case Tick =>
      val msg = SSEStatistics(droppedMessageCount)
      enqueue(msg, diagnosticMessages)
    case msg: SSEShutdown =>
      log.info(s"Closing SSE stream: ${msg.reason}")
      enqueue(msg, diagnosticMessages)
      shutdown = true
    case msg: SSEMessage =>
      enqueue(msg, messages)
  }

  private def enqueue(msg: SSEMessage, queue: BlockingQueue[String]): Unit = {
    val str = msg.toSSE + "\r\n\r\n"
    if (queue.offer(str)) {
      registry.counter(messagesId.withTag("action", msg.getWhat)).increment()
      registry.counter(messageBytesId.withTag("action", msg.getWhat)).increment(str.length)
    } else {
      droppedMessageCount += 1
      registry.counter(droppedId.withTag("action", msg.getWhat)).increment()
    }
    writeChunks()
  }

  def writeChunks(): Unit = {
    writeChunks(diagnosticMessages)
    writeChunks(messages)

    // If we have received a shutdown message, wait until we have flushed all
    // diagnostic messages and then exit.
    if (shutdown && diagnosticMessages.isEmpty) {
      onCompleteThenStop()
    }
  }

  def writeChunks(queue: BlockingQueue[String]): Unit = {
    while (totalDemand > 0L && !queue.isEmpty) {
      val str = queue.take()
      registry.counter(sendBytesId).increment(str.length)
      registry.counter(sendsId).increment()
      onNext(ChunkStreamPart(str))
    }
  }

  private def unregister(): Unit = {
    if (needsUnregister) {
      needsUnregister = false
      sm.unregister(sseId)
      sseCount.decrementAndGet()
    }
  }

  override def postStop(): Unit = {
    unregister()
    ticker.cancel()
    super.postStop()
  }
}

object SSEActor {

  case object Tick
}
