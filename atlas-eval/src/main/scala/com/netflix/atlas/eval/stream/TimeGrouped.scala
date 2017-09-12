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
package com.netflix.atlas.eval.stream

import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import com.netflix.atlas.eval.model.TimeGroup

/**
  * Operator for grouping data into buckets by time. The expectation is that the data
  * is roughly time ordered and that data from later times is a good indicator that
  * older times are complete.
  *
  * @param numBuffers
  *     Number of time buffers to maintain. The buffers are stored in a rolling array
  *     and the data for a given buffer will be emitted when the first data comes in
  *     for a new time that would evict the buffer with the minimum time.
  * @param max
  *     Maximum number of items that can be accumulated for a given time.
  * @param ts
  *     Function that extracts the timestamp for an item.
  * @tparam T
  *     Item from the input stream.
  */
private[stream] class TimeGrouped[T](numBuffers: Int, max: Int, ts: T => Long)
    extends GraphStage[FlowShape[T, TimeGroup[T]]] {

  private val in = Inlet[T]("TimeGrouped.in")
  private val out = Outlet[TimeGroup[T]]("TimeGrouped.out")

  override val shape: FlowShape[T, TimeGroup[T]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private val buf = new Array[List[T]](numBuffers)
      buf.indices.foreach { i =>
        buf(i) = Nil
      }

      private val timestamps = new Array[Long](numBuffers)

      private var cutoffTime = 0L

      private var pending: List[TimeGroup[T]] = Nil

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
        val t = ts(v)
        if (t <= cutoffTime) pull(in)
        else {
          val i = findBuffer(t)
          if (i >= 0) {
            buf(i) = v :: buf(i)
            pull(in)
          } else {
            val pos = -i - 1
            val vs = buf(pos)
            if (vs.nonEmpty) push(out, TimeGroup(timestamps(pos), vs)) else pull(in)
            cutoffTime = timestamps(pos)
            buf(pos) = List(v)
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
        val groups = buf.indices.map(i => TimeGroup(timestamps(i), buf(i))).toList
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
