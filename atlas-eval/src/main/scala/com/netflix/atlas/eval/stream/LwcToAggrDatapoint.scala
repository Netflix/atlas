/*
 * Copyright 2014-2021 Netflix, Inc.
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
    extends GraphStage[FlowShape[AnyRef, AggrDatapoint]] {

  private val in = Inlet[AnyRef]("LwcToAggrDatapoint.in")
  private val out = Outlet[AggrDatapoint]("LwcToAggrDatapoint.out")

  override val shape: FlowShape[AnyRef, AggrDatapoint] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private[this] val state = scala.collection.mutable.AnyRefMap.empty[String, LwcDataExpr]

      // HACK: needed until we can plumb the actual source through the system
      private var nextSource: Int = 0

      override def onPush(): Unit = {
        grab(in) match {
          case sb: LwcSubscription      => updateState(sb)
          case dp: LwcDatapoint         => pushDatapoint(dp)
          case dg: LwcDiagnosticMessage => pushDiagnosticMessage(dg)
          case hb: LwcHeartbeat         => pushHeartbeat(hb)
          case _                        => pull(in)
        }
      }

      private def updateState(sub: LwcSubscription): Unit = {
        sub.metrics.foreach { m =>
          if (!state.contains(m.id)) {
            state.put(m.id, m)
          }
        }
        pull(in)
      }

      private def pushDatapoint(dp: LwcDatapoint): Unit = {
        state.get(dp.id) match {
          case Some(sub) =>
            // TODO, put in source, for now make it random to avoid dedup
            nextSource += 1
            val expr = sub.expr
            val step = sub.step
            push(
              out,
              AggrDatapoint(dp.timestamp, step, expr, nextSource.toString, dp.tags, dp.value)
            )
          case None =>
            pull(in)
        }
      }

      private def pushDiagnosticMessage(diagMsg: LwcDiagnosticMessage): Unit = {
        state.get(diagMsg.id).foreach { sub =>
          context.log(sub.expr, diagMsg.message)
        }
        pull(in)
      }

      private def pushHeartbeat(hb: LwcHeartbeat): Unit = {
        push(out, AggrDatapoint.heartbeat(hb.timestamp, hb.step))
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
