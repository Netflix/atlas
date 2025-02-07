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

import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.DataVocabulary
import com.netflix.atlas.core.model.EventExpr
import com.netflix.atlas.core.model.EventVocabulary
import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.TraceVocabulary
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
import com.netflix.atlas.eval.model.LwcSubscriptionV2
import com.netflix.atlas.eval.model.DatapointsTuple
import com.netflix.atlas.eval.model.ExprType
import com.netflix.atlas.eval.model.LwcDataExpr

/**
  * Process the SSE output from an LWC service and convert it into a stream of
  * [[AggrDatapoint]]s that can be used for evaluation.
  */
private[stream] class LwcToAggrDatapoint(context: StreamContext)
    extends GraphStage[FlowShape[List[AnyRef], DatapointsTuple]] {

  import LwcToAggrDatapoint.*

  private val unknown = context.registry.counter("atlas.eval.unknownMessages")

  private val in = Inlet[List[AnyRef]]("LwcToAggrDatapoint.in")
  private val out = Outlet[DatapointsTuple]("LwcToAggrDatapoint.out")

  override val shape: FlowShape[List[AnyRef], DatapointsTuple] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private val tsState = scala.collection.mutable.HashMap.empty[String, DatapointMetadata]
      private val eventState = scala.collection.mutable.HashMap.empty[String, String]

      override def onPush(): Unit = {
        val dpBuilder = List.newBuilder[AggrDatapoint]
        val msgBuilder = List.newBuilder[Evaluator.MessageEnvelope]
        grab(in).foreach {
          case sb: LwcSubscription      => updateState(sb)
          case sb: LwcSubscriptionV2    => updateStateV2(sb)
          case dp: LwcDatapoint         => dpBuilder ++= pushDatapoint(dp)
          case ev: LwcEvent             => msgBuilder ++= pushEvent(ev)
          case dg: LwcDiagnosticMessage => msgBuilder ++= pushDiagnosticMessage(dg)
          case hb: LwcHeartbeat         => dpBuilder += pushHeartbeat(hb)
          case _                        =>
        }
        val datapoints = dpBuilder.result()
        val messages = msgBuilder.result()
        if (datapoints.isEmpty && messages.isEmpty)
          pull(in)
        else
          push(out, DatapointsTuple(datapoints, messages))
      }

      private def updateState(sub: LwcSubscription): Unit = {
        sub.metrics.foreach { m =>
          if (!tsState.contains(m.id)) {
            val expr = parseExpr(m.expression)
            tsState.put(m.id, DatapointMetadata(m.expression, expr, m.step))
          }
        }
      }

      private def updateStateV2(sub: LwcSubscriptionV2): Unit = {
        sub.subExprs.foreach { s =>
          if (isTimeSeries(sub, s)) {
            if (!tsState.contains(s.id)) {
              val expr = parseExpr(s.expression, sub.exprType)
              tsState.put(s.id, DatapointMetadata(s.expression, expr, s.step))
            }
          } else if (sub.exprType.isEventType && !eventState.contains(s.id)) {
            eventState.put(s.id, s.expression)
          }
        }
      }

      private def isTimeSeries(sub: LwcSubscriptionV2, s: LwcDataExpr): Boolean = {
        // The sample type behaves similar to a time series for most processing, but maintains
        // some sample events during aggregation.
        sub.exprType.isTimeSeriesType || s.expression.contains(",:sample")
      }

      private def pushDatapoint(dp: LwcDatapoint): Option[AggrDatapoint] = {
        tsState.get(dp.id) match {
          case Some(sub) =>
            val expr = sub.dataExpr
            val step = sub.step
            Some(
              AggrDatapoint(
                dp.timestamp,
                step,
                expr,
                sub.dataExprStr,
                dp.tags,
                dp.value,
                dp.samples
              )
            )
          case None =>
            unknown.increment()
            None
        }
      }

      private def pushEvent(event: LwcEvent): List[Evaluator.MessageEnvelope] = {
        eventState.get(event.id) match {
          case Some(sub) => context.messagesForDataSource(sub, EventMessage(event.payload))
          case None      => unknown.increment(); Nil
        }
      }

      private def pushDiagnosticMessage(
        diagMsg: LwcDiagnosticMessage
      ): List[Evaluator.MessageEnvelope] = {
        tsState.get(diagMsg.id) match {
          case Some(sub) =>
            context.messagesForDataSource(sub.dataExprStr, diagMsg.message)
          case None =>
            eventState.get(diagMsg.id) match {
              case Some(sub) => context.messagesForDataSource(sub, diagMsg.message)
              case None      => unknown.increment(); Nil
            }
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

  private val dataInterpreter = Interpreter(DataVocabulary.allWords)
  private val traceInterpreter = Interpreter(TraceVocabulary.allWords)
  private val eventInterpreter = Interpreter(EventVocabulary.allWords)

  private def parseExpr(input: String, exprType: ExprType = ExprType.TIME_SERIES): DataExpr = {
    exprType match {
      case ExprType.TIME_SERIES =>
        dataInterpreter.execute(input).stack match {
          case (expr: DataExpr) :: Nil => expr
          case _ => throw new IllegalArgumentException(s"invalid expr: $input")
        }
      case ExprType.TRACE_TIME_SERIES =>
        traceInterpreter.execute(input).stack match {
          case ModelExtractors.TraceTimeSeriesType(tq) :: Nil => tq.expr.expr.dataExprs.head
          case _ => throw new IllegalArgumentException(s"invalid expr: $input")
        }
      case ExprType.EVENTS =>
        eventInterpreter.execute(input).stack match {
          case ModelExtractors.EventExprType(expr: EventExpr.Sample) :: Nil => expr.dataExpr
          case _ => throw new IllegalArgumentException(s"invalid expr: $input")
        }
      case _ =>
        throw new IllegalArgumentException(s"unsupported expression type: $exprType")
    }
  }
}
