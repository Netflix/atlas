/*
 * Copyright 2014-2022 Netflix, Inc.
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
package com.netflix.atlas.eval.stream

import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.AggrValuesInfo
import com.netflix.atlas.eval.model.TimeGroup

/**
  * Operator for grouping data into buckets by time. The expectation is that the data
  * is roughly time ordered and that data from later times is a good indicator that
  * older times are complete.
  *
  * @param context
  *     Shared context for the evaluation stream.
  */
private[stream] class TimeGrouped(
  context: StreamContext
) extends GraphStage[FlowShape[AggrDatapoint, TimeGroup]] {

  type AggrMap = scala.collection.mutable.AnyRefMap[DataExpr, AggrDatapoint.Aggregator]

  /**
    * Number of time buffers to maintain. The buffers are stored in a rolling array
    * and the data for a given buffer will be emitted when the first data comes in
    * for a new time that would evict the buffer with the minimum time.
    */
  private val numBuffers = context.numBuffers

  private val maxInputDatapointsPerExpression = context.maxInputDatapointsPerExpression

  private val maxIntermediateDatapointsPerExpression =
    context.maxIntermediateDatapointsPerExpression

  private val aggrSettings = AggrDatapoint.AggregatorSettings(
    maxInputDatapointsPerExpression,
    maxIntermediateDatapointsPerExpression,
    context.registry
  )

  private val in = Inlet[AggrDatapoint]("TimeGrouped.in")
  private val out = Outlet[TimeGroup]("TimeGrouped.out")

  override val shape: FlowShape[AggrDatapoint, TimeGroup] = FlowShape(in, out)

  private val metricName = "atlas.eval.datapoints"
  private val registry = context.registry
  private val droppedOld = registry.counter(metricName, "id", "dropped-old")
  private val droppedFuture = registry.counter(metricName, "id", "dropped-future")
  private val buffered = registry.counter(metricName, "id", "buffered")
  private val clock = registry.clock()

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private val droppedOldUpdater = droppedOld.batchUpdater(10_000)
      private val droppedFutureUpdater = droppedFuture.batchUpdater(10_000)
      private val bufferedUpdater = buffered.batchUpdater(10_000)

      private val buf = new Array[AggrMap](numBuffers)
      buf.indices.foreach { i =>
        buf(i) = new AggrMap
      }

      private val timestamps = new Array[Long](numBuffers)

      private var step = -1L
      private var cutoffTime = 0L

      private var pending: List[TimeGroup] = Nil

      private def findBuffer(t: Long): Int = {
        var i = 0
        var min = 0
        while (i < timestamps.length) {
          if (timestamps(i) == t) return i
          if (i > 0 && timestamps(i) < timestamps(i - 1)) min = i
          i += 1
        }
        -min - 1
      }

      /**
        * Add a value to the aggregate or create a new aggregate initialized to the provided
        * value. Heartbeat datapoints will be ignored as they are just used to trigger flushing
        * of the time group.
        */
      private def aggregate(i: Int, v: AggrDatapoint): Unit = {
        if (!v.isHeartbeat) {
          buf(i).get(v.expr) match {
            case Some(aggr) => aggr.aggregate(v)
            case None       => buf(i).put(v.expr, AggrDatapoint.newAggregator(v, aggrSettings))
          }
        }
      }

      /**
        * Push the most recently completed time group to the next stage and reset the buffer
        * so it can be used for a new time window.
        */
      private def flush(i: Int): Unit = {
        val t = timestamps(i)
        if (t > 0) push(out, toTimeGroup(t, buf(i))) else pull(in)
        cutoffTime = t
        buf(i) = new AggrMap
      }

      private def toTimeGroup(ts: Long, aggrMap: AggrMap): TimeGroup = {
        val aggregateMapForExpWithinLimits = aggrMap
          .filter {
            case (expr, aggr) if aggr.limitExceeded =>
              context.logDatapointsExceeded(ts, expr)
              false
            case _ =>
              true
          }
          .map {
            case (expr, aggr) => expr -> AggrValuesInfo(aggr.datapoints, aggr.numInputDatapoints)
          }
          .toMap

        TimeGroup(ts, step, aggregateMapForExpWithinLimits)
      }

      override def onPush(): Unit = {
        val v = grab(in)
        val t = v.timestamp
        val now = clock.wallTime()
        step = v.step
        if (t > now) {
          droppedFutureUpdater.increment()
          pull(in)
        } else if (t <= cutoffTime) {
          droppedOldUpdater.increment()
          pull(in)
        } else {
          bufferedUpdater.increment()
          val i = findBuffer(t)
          if (i >= 0) {
            aggregate(i, v)
            pull(in)
          } else {
            val pos = -i - 1
            flush(pos)
            aggregate(pos, v)
            timestamps(pos) = t
          }
        }
      }

      override def onPull(): Unit = {
        if (isClosed(in))
          flushPending()
        else
          pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        val groups = buf.indices.map { i =>
          toTimeGroup(timestamps(i), buf(i))
        }.toList
        pending = groups.filter(_.timestamp > 0).sortWith(_.timestamp < _.timestamp)
        flushPending()
        droppedOldUpdater.close()
        droppedFutureUpdater.close()
        bufferedUpdater.close()
      }

      private def flushPending(): Unit = {
        if (pending.nonEmpty && isAvailable(out)) {
          push(out, pending.head)
          pending = pending.tail
        }
        if (pending.isEmpty) {
          completeStage()
        }
      }

      setHandlers(in, out, this)
    }
  }
}
