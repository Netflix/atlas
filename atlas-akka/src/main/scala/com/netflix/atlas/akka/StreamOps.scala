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
package com.netflix.atlas.akka

import java.util.concurrent.TimeUnit

import akka.Done
import akka.NotUsed
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.OverflowStrategy
import akka.stream.QueueOfferResult
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Timer

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

/**
  * Utility functions for commonly used operations on Akka streams. Most of these are for
  * the purpose of getting additional instrumentation into the behavior of the stream.
  */
object StreamOps {

  /**
    * Wraps a source queue and adds monitoring for the results of items offered to the queue.
    * This can be used to detect if items are being dropped or offered after the associated
    * stream has been closed.
    *
    * @param registry
    *     Spectator registry to manage metrics for this queue.
    * @param id
    *     Dimension used to distinguish a particular queue usage.
    * @param size
    *     Number of enqueued items to allow before triggering the overflow strategy.
    * @param strategy
    *     How to handle items that come in while the queue is full.
    */
  def queue[T](
    registry: Registry,
    id: String,
    size: Int,
    strategy: OverflowStrategy
  ): Source[T, SourceQueueWithComplete[T]] = {
    Source
      .queue[T](size, strategy)
      .mapMaterializedValue(q => new WrappedSourceQueueWithComplete[T](registry, id, q))
  }

  private final class WrappedSourceQueueWithComplete[T](
    registry: Registry,
    id: String,
    queue: SourceQueueWithComplete[T]
  ) extends SourceQueueWithComplete[T] {

    private implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

    private val baseId = registry.createId("akka.stream.offeredToQueue", "id", id)
    private val enqueued = registry.counter(baseId.withTag("result", "enqueued"))
    private val dropped = registry.counter(baseId.withTag("result", "droppedQueueFull"))
    private val closed = registry.counter(baseId.withTag("result", "droppedQueueClosed"))
    private val failed = registry.counter(baseId.withTag("result", "droppedQueueFailure"))
    private val completed = registry.counter(baseId.withTag("result", "droppedStreamCompleted"))

    override def complete(): Unit = queue.complete()

    override def fail(ex: Throwable): Unit = queue.fail(ex)

    override def watchCompletion(): Future[Done] = queue.watchCompletion()

    override def offer(elem: T): Future[QueueOfferResult] = {
      queue.offer(elem).andThen {
        case Success(QueueOfferResult.Enqueued)    => enqueued.increment()
        case Success(QueueOfferResult.Dropped)     => dropped.increment()
        case Success(QueueOfferResult.QueueClosed) => closed.increment()
        case Success(QueueOfferResult.Failure(_))  => failed.increment()
        case Failure(t)                            => completed.increment()
      }
    }
  }

  /**
    * Stage that measures the flow of data through the stream. It will keep track of three
    * meters:
    *
    * - `akka.stream.numEvents`: a simple counter indicating the number of events that flow
    *   through the stream.
    *
    * - `akka.stream.downstreamDelay`: a timer measuring the delay for the downstream to
    *   request a new element from the stream after an item is pushed. This can be used to
    *   understand if the downstream cannot keep up and is introducing significant back
    *   pressure delays.
    *
    * - `akka.stream.upstreamDelay`: a timer measuring the delay for the upstream to push
    *   a new element after one has been requested.
    *
    * @param registry
    *     Spectator registry to manage metrics.
    * @param id
    *     Dimension used to distinguish a particular usage.
    */
  def monitorFlow[T](registry: Registry, id: String): Flow[T, T, NotUsed] = {
    Flow[T].via(new MonitorFlow[T](registry, id))
  }

  private final class MonitorFlow[T](registry: Registry, id: String)
      extends GraphStage[FlowShape[T, T]] {

    private val numEvents = registry.counter("akka.stream.numEvents", "id", id)
    private val upstreamTimer = registry.timer("akka.stream.upstreamDelay", "id", id)
    private val downstreamTimer = registry.timer("akka.stream.downstreamDelay", "id", id)

    private val in = Inlet[T]("MonitorBackpressure.in")
    private val out = Outlet[T]("MonitorBackpressure.out")

    override val shape: FlowShape[T, T] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

      new GraphStageLogic(shape) with InHandler with OutHandler {

        private var upstreamStart: Long = -1L
        private var downstreamStart: Long = -1L

        override def onPush(): Unit = {
          upstreamStart = record(upstreamTimer, upstreamStart)
          push(out, grab(in))
          numEvents.increment()
          downstreamStart = registry.clock().monotonicTime()
        }

        override def onPull(): Unit = {
          downstreamStart = record(downstreamTimer, downstreamStart)
          pull(in)
          upstreamStart = registry.clock().monotonicTime()
        }

        private def record(timer: Timer, start: Long): Long = {
          if (start > 0L) {
            val delay = registry.clock().monotonicTime() - start
            timer.record(delay, TimeUnit.NANOSECONDS)
          }
          -1L
        }

        setHandlers(in, out, this)
      }
    }
  }
}
