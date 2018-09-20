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
import com.netflix.atlas.eval.model.TimeGroup

/**
  * Operator for grouping data into buckets by time. The expectation is that the data
  * is roughly time ordered and that data from later times is a good indicator that
  * older times are complete.
  *
  * @param context
  *     Shared context for the evaluation stream.
  * @param max
  *     Maximum number of items that can be accumulated for a given time.
  */
private[stream] class TimeGrouped(
  context: StreamContext,
  max: Int,
) extends GraphStage[FlowShape[AggrDatapoint, TimeGroup[AggrDatapoint]]] {

  type AggrMap = scala.collection.mutable.AnyRefMap[DataExpr, AggrDatapoint.Aggregator]

  /**
    * Number of time buffers to maintain. The buffers are stored in a rolling array
    * and the data for a given buffer will be emitted when the first data comes in
    * for a new time that would evict the buffer with the minimum time.
    */
  private val numBuffers = context.numBuffers

  private val in = Inlet[AggrDatapoint]("TimeGrouped.in")
  private val out = Outlet[TimeGroup[AggrDatapoint]]("TimeGrouped.out")

  override val shape: FlowShape[AggrDatapoint, TimeGroup[AggrDatapoint]] = FlowShape(in, out)

  private val metricName = "atlas.eval.datapoints"
  private val droppedOld = context.registry.counter(metricName, "id", "dropped-old")
  private val droppedFuture = context.registry.counter(metricName, "id", "dropped-future")
  private val buffered = context.registry.counter(metricName, "id", "buffered")

  private val clock = context.registry.clock()

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private val buf = new Array[AggrMap](numBuffers)
      buf.indices.foreach { i =>
        buf(i) = new AggrMap
      }

      private val timestamps = new Array[Long](numBuffers)

      private var cutoffTime = 0L

      private var pending: List[TimeGroup[AggrDatapoint]] = Nil

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

      override def onPush(): Unit = {
        val v = grab(in)
        val t = v.timestamp
        val now = clock.wallTime()
        if (t > now) {
          droppedFuture.increment()
          pull(in)
        } else if (t <= cutoffTime) {
          droppedOld.increment()
          pull(in)
        } else {
          buffered.increment()
          val i = findBuffer(t)
          if (i >= 0) {
            buf(i).get(v.expr) match {
              case Some(aggr) => aggr.aggregate(v)
              case None       => buf(i).put(v.expr, AggrDatapoint.newAggregator(v))
            }
            pull(in)
          } else {
            val pos = -i - 1
            val vs = buf(pos).values.flatMap(_.datapoints).toList
            if (vs.nonEmpty) push(out, TimeGroup(timestamps(pos), vs)) else pull(in)
            cutoffTime = timestamps(pos)
            buf(pos) = new AggrMap
            buf(pos).put(v.expr, AggrDatapoint.newAggregator(v))
            timestamps(pos) = t
          }
        }
      }

      override def onPull(): Unit = {
        if (isClosed(in))
          flush()
        else
          pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        val groups = buf.indices.map { i =>
          val vs = buf(i).values.flatMap(_.datapoints).toList
          TimeGroup(timestamps(i), vs)
        }.toList
        pending = groups.filter(_.values.nonEmpty).sortWith(_.timestamp < _.timestamp)
        flush()
      }

      private def flush(): Unit = {
        if (pending.nonEmpty) {
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
