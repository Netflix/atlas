/*
 * Copyright 2014-2025 Netflix, Inc.
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

import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.FlowShape
import org.apache.pekko.stream.Inlet
import org.apache.pekko.stream.Outlet
import org.apache.pekko.stream.stage.GraphStage
import org.apache.pekko.stream.stage.GraphStageLogic
import org.apache.pekko.stream.stage.InHandler
import org.apache.pekko.stream.stage.OutHandler
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.LwcDataExpr
import com.netflix.atlas.eval.model.LwcDatapoint
import com.netflix.atlas.eval.model.LwcDiagnosticMessage
import com.netflix.atlas.eval.model.LwcHeartbeat
import com.netflix.atlas.eval.model.LwcSubscription

/**
  * Process the SSE output from an LWC service and convert it into a stream of
  * [[AggrDatapoint]]s that can be used for evaluation.
  */
private[stream] class LwcToAggrDatapoint(context: StreamContext)
    extends GraphStage[FlowShape[List[AnyRef], List[AggrDatapoint]]] {

  private val in = Inlet[List[AnyRef]]("LwcToAggrDatapoint.in")
  private val out = Outlet[List[AggrDatapoint]]("LwcToAggrDatapoint.out")

  override val shape: FlowShape[List[AnyRef], List[AggrDatapoint]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private[this] val state = scala.collection.mutable.HashMap.empty[String, LwcDataExpr]

      // HACK: needed until we can plumb the actual source through the system
      private var nextSource: Int = 0

      override def onPush(): Unit = {
        val builder = List.newBuilder[AggrDatapoint]
        grab(in).foreach {
          case sb: LwcSubscription      => updateState(sb)
          case dp: LwcDatapoint         => builder ++= pushDatapoint(dp)
          case dg: LwcDiagnosticMessage => pushDiagnosticMessage(dg)
          case hb: LwcHeartbeat         => builder += pushHeartbeat(hb)
          case _                        =>
        }
        val datapoints = builder.result()
        if (datapoints.isEmpty)
          pull(in)
        else
          push(out, datapoints)
      }

      private def updateState(sub: LwcSubscription): Unit = {
        sub.metrics.foreach { m =>
          if (!state.contains(m.id)) {
            state.put(m.id, m)
          }
        }
      }

      private def pushDatapoint(dp: LwcDatapoint): Option[AggrDatapoint] = {
        state.get(dp.id).map { sub =>
          // TODO, put in source, for now make it random to avoid dedup
          nextSource += 1
          val expr = sub.expr
          val step = sub.step
          AggrDatapoint(dp.timestamp, step, expr, nextSource.toString, dp.tags, dp.value)
        }
      }

      private def pushDiagnosticMessage(diagMsg: LwcDiagnosticMessage): Unit = {
        state.get(diagMsg.id).foreach { sub =>
          context.log(sub.expr, diagMsg.message)
        }
      }

      private def pushHeartbeat(hb: LwcHeartbeat): AggrDatapoint = {
        AggrDatapoint.heartbeat(hb.timestamp, hb.step)
      }

      override def onPull(): Unit = {
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        completeStage()
      }

      setHandlers(in, out, this)
    }
  }
}
