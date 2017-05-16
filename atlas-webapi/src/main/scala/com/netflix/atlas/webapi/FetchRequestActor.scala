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
package com.netflix.atlas.webapi

import java.time.Instant
import java.time.temporal.ChronoUnit

import akka.actor.ActorLogging
import akka.actor.ActorRefFactory
import akka.actor.Props
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpEntity.ChunkStreamPart
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage._
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.akka.CustomMediaTypes._
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.StatefulExpr
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.webapi.GraphApi.DataResponse


/**
  * Provides the SSE data stream payload for a fetch response. Fetch is an alternative
  * to the graph API that is meant for accessing the data rather than rendering as an
  * image. The response can be partitioned across both the set of expressions and across
  * time to allow more flexibility on the backend for tradeoffs between latency and
  * intermediate overhead. The graph API enforces strict limits on the sizes.
  */
class FetchRequestActor(request: GraphApi.Request)
  extends ActorPublisher[ChunkStreamPart] with ActorLogging {

  import FetchRequestActor._

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  // When fetching the data for a group by many results will come back at once. This
  // queue is used to hold onto them until the consumer of the stream has enough capacity
  // read them.
  private val queue = new java.util.ArrayDeque[JsonSupport](1024)

  // Indicates the evaluation is done. There may still be messages in the queue that have
  // not been flushed to the consumer.
  private var done = false

  // Ensure that there is regular activity on the socket. In some cases the evaluation may
  // take a while to get to a point where it can emit data. This is also used to provide some
  // context to the consumer such as the percentage of the chunks that have been written out.
  private val ticker = context.system.scheduler.schedule(10.seconds, 10.seconds, self, Tick)

  private val dbRef = context.actorSelection("/user/db")

  // Set of chunks computed from the input time range
  private var chunks = {
    val step = request.roundedStepSize
    val (fstart, fend) = roundToStep(step, request.resStart, request.resEnd)
    EvalContext(fstart.toEpochMilli, fend.toEpochMilli, step)
      .partition(60 * step, ChronoUnit.MILLIS)
  }

  // Overall number of chunks that need to be processed. Used to compute a percent
  // for the tick message.
  private val numChunks = chunks.size

  // Current chunk that is being evaluated
  private var chunk: EvalContext = _

  // State for the evaluation, used to carry forward the progress of stateful operators
  // as we evaluate the chunks. Note that the chunks must be evaluated in time order
  // for the state to be correct.
  private var state = Map.empty[StatefulExpr, Any]

  def receive: Receive = {
    case Request(_) =>
      writeChunks()
    case Cancel =>
      onCompleteThenStop()

    case Acquired if chunks.isEmpty =>
      queue.addLast(DiagnosticMessage.close)
      done = true
      writeChunks()
    case Acquired if chunks.nonEmpty && chunk == null =>
      chunk = chunks.head.copy(state = state)
      chunks = chunks.tail
      try {
        dbRef ! request.toDbRequest.copy(context = chunk)
      } catch {
        case e: Exception =>
          enqueue(DiagnosticMessage.error(e))
          chunks = Nil
      }
    case DataResponse(data) =>
      request.exprs.foreach { s =>
        val result = s.expr.eval(chunk, data)
        state = result.state
        result.data.foreach { ts =>
          queue.add(TimeSeriesMessage(s.toString, chunk, ts))
        }
      }
      chunk = null
      writeChunks()

    case Tick =>
      val pct = 100.0 - 100.0 * chunks.size / numChunks.toDouble
      val msg = DiagnosticMessage.info(f"$pct%.1f%% of data complete")
      enqueue(msg)
  }

  override def postStop(): Unit = {
    ticker.cancel()
    super.postStop()
  }

  private def enqueue(msg: JsonSupport): Unit = {
    if (!done) {
      queue.addLast(msg)
    }
    writeChunks()
  }

  private def writeChunk(msg: JsonSupport): Unit = {
    val bytes = ByteString(s"$prefix${msg.toJson}$suffix")
    onNext(ChunkStreamPart(bytes))
  }

  private def writeChunks(): Unit = {
    while (totalDemand > 0 && !queue.isEmpty) {
      writeChunk(queue.pollFirst())
    }
    if (queue.isEmpty && done) {
      onCompleteThenStop()
    } else if (queue.isEmpty) {
      self ! Acquired
    }
  }
}

object FetchRequestActor {

  // SSE message prefix and suffix. Fetch is simple and just emits data items with JSON.
  private val prefix = "data: "
  private val suffix = "\n\n"

  // Message received when the actor acquires the token to proceed. TODO: the internal
  // throttling logic still needs to be ported...
  case object Acquired

  // Message received at regular interval from the scheduler
  case object Tick

  /**
    * Returns an HttpResponse with an entity that is generated by the FetchRequestActor.
    */
  def createResponse(system: ActorRefFactory, request: GraphApi.Request): HttpResponse = {
    val source = Source.actorPublisher(Props(new FetchRequestActor(request)))
    HttpResponse(
      status = StatusCodes.OK,
      entity = HttpEntity.Chunked(`text/event-stream`, source)
    )
  }

  private def roundToStep(step: Long, s: Instant, e: Instant): (Instant, Instant) = {
    val rs = roundToStep(step, s)
    val re = roundToStep(step, e)
    val adjustedStart = if (rs.equals(re)) rs.minusMillis(step) else rs
    adjustedStart -> re
  }

  private def roundToStep(step: Long, i: Instant): Instant = {
    Instant.ofEpochMilli(i.toEpochMilli / step * step)
  }
}
