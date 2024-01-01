/*
 * Copyright 2014-2024 Netflix, Inc.
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
package com.netflix.atlas.pekko

import java.util.concurrent.TimeUnit
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.ActorAttributes
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.BoundedSourceQueue
import org.apache.pekko.stream.FlowShape
import org.apache.pekko.stream.Graph
import org.apache.pekko.stream.Inlet
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.Outlet
import org.apache.pekko.stream.QueueOfferResult
import org.apache.pekko.stream.SourceShape
import org.apache.pekko.stream.Supervision
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.stage.GraphStage
import org.apache.pekko.stream.stage.GraphStageLogic
import org.apache.pekko.stream.stage.InHandler
import org.apache.pekko.stream.stage.OutHandler
import org.apache.pekko.stream.stage.TimerGraphStageLogic
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.FiniteDuration

/**
  * Utility functions for commonly used operations on Pekko streams. Most of these are for
  * the purpose of getting additional instrumentation into the behavior of the stream.
  */
object StreamOps extends StrictLogging {

  /**
    * Return attributes for running a stream with a supervision strategy that will provide
    * some insight into exceptions.
    *
    * @param registry
    *     Registry to use for reporting metrics.
    */
  def supervisionStrategy(registry: Registry): Attributes = {
    ActorAttributes.withSupervisionStrategy { t =>
      registry
        .counter("pekko.stream.exceptions", "error", t.getClass.getSimpleName)
        .increment()
      logger.warn(s"exception from stream stage", t)
      Supervision.Stop
    }
  }

  /**
    * Creates a queue source based on a `BoundedSourceQueue`. Values offered to the queue
    * will be emitted by the source. Can be used to get metrics when using `Source.queue(size)`
    * that was introduced in [#29770].
    *
    * [#29770]: https://github.com/pekko/pekko/pull/29770
    *
    * The bounded source queue should be preferred to `Source.queue(*, strategy)`
    * or `Source.actorRef` that can have unbounded memory use if the consumer cannot keep
    * up with the rate of data being offered ([#25798]).
    *
    * [#25798]: https://github.com/pekko/pekko/issues/25798
    *
    * @param registry
    *     Spectator registry to manage metrics for this queue.
    * @param id
    *     Dimension used to distinguish a particular queue usage.
    * @param size
    *     Number of enqueued items to allow before triggering the overflow strategy.
    * @return
    *     Source that emits values offered to the queue.
    */
  def blockingQueue[T](registry: Registry, id: String, size: Int): Source[T, SourceQueue[T]] = {
    Source.queue(size).mapMaterializedValue(q => new SourceQueue[T](registry, id, q))
  }

  final class SourceQueue[T] private[pekko] (
    registry: Registry,
    id: String,
    queue: BoundedSourceQueue[T]
  ) {

    private val baseId = registry.createId("pekko.stream.offeredToQueue", "id", id)
    private val enqueued = registry.counter(baseId.withTag("result", "enqueued"))
    private val dropped = registry.counter(baseId.withTag("result", "droppedQueueFull"))
    private val closed = registry.counter(baseId.withTag("result", "droppedQueueClosed"))
    private val failed = registry.counter(baseId.withTag("result", "droppedQueueFailure"))

    @volatile private var completed: Boolean = false

    /**
      * Add the value into the queue if there is room. Returns true if the value was successfully
      * enqueued.
      */
    def offer(value: T): Boolean = {
      queue.offer(value) match {
        case QueueOfferResult.Enqueued    => enqueued.increment(); true
        case QueueOfferResult.Dropped     => dropped.increment(); false
        case QueueOfferResult.QueueClosed => closed.increment(); false
        case QueueOfferResult.Failure(_)  => failed.increment(); false
      }
    }

    /**
      * Indicate that the use of the queue is complete. This will allow the associated stream
      * to finish processing elements and then shutdown. Any new elements offered to the queue
      * will be dropped.
      */
    def complete(): Unit = {
      queue.complete()
      completed = true
    }

    /** Check if the queue is open to take more data. */
    def isOpen: Boolean = !completed

    /** The approximate number of entries in the queue. */
    def size: Int = queue.size()
  }

  /**
    * Stage that measures the flow of data through the stream. It will keep track of three
    * meters:
    *
    * - `pekko.stream.numEvents`: a simple counter indicating the number of events that flow
    *   through the stream.
    *
    * - `pekko.stream.downstreamDelay`: a timer measuring the delay for the downstream to
    *   request a new element from the stream after an item is pushed. This can be used to
    *   understand if the downstream cannot keep up and is introducing significant back
    *   pressure delays.
    *
    * - `pekko.stream.upstreamDelay`: a timer measuring the delay for the upstream to push
    *   a new element after one has been requested.
    *
    * Updates to meters will be batched so the updates may be delayed if used for a low
    * throughput stream.
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

    private val numEvents = registry.counter("pekko.stream.numEvents", "id", id)
    private val upstreamTimer = registry.timer("pekko.stream.upstreamDelay", "id", id)
    private val downstreamTimer = registry.timer("pekko.stream.downstreamDelay", "id", id)

    private val in = Inlet[T]("MonitorBackpressure.in")
    private val out = Outlet[T]("MonitorBackpressure.out")

    override val shape: FlowShape[T, T] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

      new GraphStageLogic(shape) with InHandler with OutHandler {

        import MonitorFlow._

        private var lastUpdate = registry.clock().monotonicTime()
        private val numEventsUpdater = numEvents.batchUpdater(MeterBatchSize)
        private val upstreamUpdater = upstreamTimer.batchUpdater(MeterBatchSize)
        private val downstreamUpdater = downstreamTimer.batchUpdater(MeterBatchSize)

        private var upstreamStart = -1L
        private var downstreamStart = -1L

        override def onPush(): Unit = {
          val now = registry.clock().monotonicTime()
          numEventsUpdater.increment()
          if (upstreamStart != -1L) {
            upstreamUpdater.record(now - upstreamStart, TimeUnit.NANOSECONDS)
            upstreamStart = -1L
          }
          push(out, grab(in))
          downstreamStart = now
          if (now - lastUpdate > MeterUpdateInterval) {
            updateMeters(now)
          }
        }

        override def onPull(): Unit = {
          val now = registry.clock().monotonicTime()
          if (downstreamStart != -1L) {
            downstreamUpdater.record(now - downstreamStart, TimeUnit.NANOSECONDS)
            downstreamStart = -1L
          }
          pull(in)
          upstreamStart = now
        }

        override def onUpstreamFinish(): Unit = {
          updateMeters(registry.clock().monotonicTime())
          numEventsUpdater.close()
          upstreamUpdater.close()
          downstreamUpdater.close()
          super.onUpstreamFinish()
        }

        private def updateMeters(now: Long): Unit = {
          numEventsUpdater.flush()
          upstreamUpdater.flush()
          downstreamUpdater.flush()
          lastUpdate = now
        }

        setHandlers(in, out, this)
      }
    }
  }

  private object MonitorFlow {

    private val MeterBatchSize = 1_000_000
    private val MeterUpdateInterval = TimeUnit.SECONDS.toNanos(1L)
  }

  /**
    * Map operation that passes in the materializer for the graph stage.
    *
    * @param f
    *     Function that maps the input value to a new value.
    * @return
    *     Flow that applies the mapping function to each value.
    */
  def map[V1, V2](f: (V1, Materializer) => V2): Flow[V1, V2, NotUsed] = {
    Flow[V1].via(new MapFlow(f))
  }

  private final class MapFlow[V1, V2](f: (V1, Materializer) => V2)
      extends GraphStage[FlowShape[V1, V2]] {

    private val in = Inlet[V1]("MapFlow.in")
    private val out = Outlet[V2]("MapFlow.out")

    override val shape: FlowShape[V1, V2] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

      new GraphStageLogic(shape) with InHandler with OutHandler {

        override def onPush(): Unit = {
          push(out, f(grab(in), materializer))
        }

        override def onPull(): Unit = {
          pull(in)
        }

        setHandlers(in, out, this)
      }
    }
  }

  /**
    * Flat map operation that passes in the materializer for the graph stage.
    *
    * @param f
    *     Function that maps the input value to a new value.
    * @return
    *     Flow that applies the mapping function to each value.
    */
  def flatMapConcat[V1, V2, M](
    f: (V1, Materializer) => Graph[SourceShape[V2], M]
  ): Flow[V1, V2, NotUsed] = {
    map(f).flatMapConcat(src => src)
  }

  /**
    * Filter out repeated values in a stream. Similar to the unix `uniq` command.
    *
    * @param timeout
    *     Repeated value will still be emitted if elapsed time since last emit exceeds
    *     timeout. Unit is milliseconds.
    */
  def unique[V](timeout: Long = Long.MaxValue, clock: Clock = Clock.SYSTEM): Flow[V, V, NotUsed] = {
    Flow[V].via(new UniqueFlow[V](timeout, clock))
  }

  private final class UniqueFlow[V](timeout: Long, clock: Clock)
      extends GraphStage[FlowShape[V, V]] {

    private val in = Inlet[V]("UniqueFlow.in")
    private val out = Outlet[V]("UniqueFlow.out")

    override val shape: FlowShape[V, V] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

      new GraphStageLogic(shape) with InHandler with OutHandler {
        private var previous: V = _
        private var lastPushedAt: Long = 0

        private def isExpired: Boolean = {
          lastPushedAt == 0 || clock.wallTime() - lastPushedAt > timeout
        }

        override def onPush(): Unit = {
          val v = grab(in)
          if (v == previous && !isExpired) {
            pull(in)
          } else {
            previous = v
            lastPushedAt = clock.wallTime()
            push(out, v)
          }
        }

        override def onPull(): Unit = {
          pull(in)
        }

        setHandlers(in, out, this)
      }
    }
  }

  /**
    * Repeat the last element that has been received. If a new element arrives, then it will be
    * pushed immediately if the outlet is ready to receive it. Otherwise it will get pushed the
    * next time the timer fires.
    *
    * @param frequency
    *     How often to push the last value when there isn't an upstream update.
    */
  def repeatLastReceived[V](frequency: FiniteDuration): Flow[V, V, NotUsed] = {
    Flow[V].via(new RepeatLastReceivedFlow[V](frequency))
  }

  private final class RepeatLastReceivedFlow[V](frequency: FiniteDuration)
      extends GraphStage[FlowShape[V, V]] {

    private val in = Inlet[V]("RepeatLastReceivedFlow.in")
    private val out = Outlet[V]("RepeatLastReceivedFlow.out")

    override val shape: FlowShape[V, V] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): TimerGraphStageLogic = {

      new TimerGraphStageLogic(shape) with InHandler with OutHandler {
        private var lastElement: V = _

        override def preStart(): Unit = {
          scheduleWithFixedDelay(NotUsed, frequency, frequency)
        }

        override def onTimer(timerKey: Any): Unit = {
          if (isAvailable(out) && lastElement != null)
            push(out, lastElement)
        }

        override def onPush(): Unit = {
          val v = grab(in)
          lastElement = v
          if (isAvailable(out))
            push(out, lastElement)
        }

        override def onPull(): Unit = {
          if (!hasBeenPulled(in))
            pull(in)
        }

        setHandlers(in, out, this)
      }
    }
  }
}
