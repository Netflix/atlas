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
package com.netflix.atlas.eval.stream

import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.DataVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.FlowShape
import org.apache.pekko.stream.Inlet
import org.apache.pekko.stream.Outlet
import org.apache.pekko.stream.stage.GraphStage
import org.apache.pekko.stream.stage.GraphStageLogic
import org.apache.pekko.stream.stage.InHandler
import org.apache.pekko.stream.stage.OutHandler
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.EventMessage
import com.netflix.atlas.eval.model.LwcDatapoint
import com.netflix.atlas.eval.model.LwcDiagnosticMessage
import com.netflix.atlas.eval.model.LwcEvent
import com.netflix.atlas.eval.model.LwcHeartbeat
import com.netflix.atlas.eval.model.LwcSubscription

/**
  * Process the SSE output from an LWC service and convert it into a stream of
  * [[AggrDatapoint]]s that can be used for evaluation.
  */
private[stream] class LwcToAggrDatapoint(context: StreamContext)
    extends GraphStage[FlowShape[List[AnyRef], List[AggrDatapoint]]] {

  import LwcToAggrDatapoint.*

  private val unknown = context.registry.counter("atlas.eval.unknownMessages")

  private val in = Inlet[List[AnyRef]]("LwcToAggrDatapoint.in")
  private val out = Outlet[List[AggrDatapoint]]("LwcToAggrDatapoint.out")

  override val shape: FlowShape[List[AnyRef], List[AggrDatapoint]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private[this] val state = scala.collection.mutable.AnyRefMap.empty[String, DatapointMetadata]

      override def onPush(): Unit = {
        val builder = List.newBuilder[AggrDatapoint]
        grab(in).foreach {
          case sb: LwcSubscription      => updateState(sb)
          case dp: LwcDatapoint         => builder ++= pushDatapoint(dp)
          case ev: LwcEvent             => pushEvent(ev)
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
            val expr = parseExpr(m.expression)
            state.put(m.id, DatapointMetadata(m.expression, expr, m.step))
          }
        }
      }

      private def pushDatapoint(dp: LwcDatapoint): Option[AggrDatapoint] = {
        state.get(dp.id) match {
          case Some(sub) =>
            val expr = sub.dataExpr
            val step = sub.step
            Some(AggrDatapoint(dp.timestamp, step, expr, "datapoint", dp.tags, dp.value))
          case None =>
            unknown.increment()
            None
        }
      }

      private def pushEvent(event: LwcEvent): Unit = {
        state.get(event.id) match {
          case Some(sub) => context.log(sub.dataExpr, EventMessage(event.payload))
          case None      => unknown.increment()
        }
      }

      private def pushDiagnosticMessage(diagMsg: LwcDiagnosticMessage): Unit = {
        state.get(diagMsg.id) match {
          case Some(sub) => context.log(sub.dataExpr, diagMsg.message)
          case None      => unknown.increment()
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

private[stream] object LwcToAggrDatapoint {

  case class DatapointMetadata(dataExprStr: String, dataExpr: DataExpr, step: Long)

  private val interpreter = Interpreter(DataVocabulary.allWords)

  private def parseExpr(input: String): DataExpr = {
    interpreter.execute(input).stack match {
      case (expr: DataExpr) :: Nil => expr
      case _                       => throw new IllegalArgumentException(s"invalid expr: $input")
    }
  }
}
