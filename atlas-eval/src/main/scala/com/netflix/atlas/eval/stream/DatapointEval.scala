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
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.StatefulExpr
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.eval.model.TimeGroup
import com.netflix.atlas.eval.model.TimeSeriesMessage

/**
  * Perform the final evaluation for an expression based on the raw datapoints
  * from the source.
  *
  * @param expr
  *     High level expression to evaluate.
  * @param step
  *     Step size for the input data.
  */
class DatapointEval[T <: TimeSeries](expr: StyleExpr, step: Long)
  extends GraphStage[FlowShape[TimeGroup[T], List[TimeSeriesMessage]]] {

  private val in = Inlet[TimeGroup[T]]("DatapointEval.in")
  private val out = Outlet[List[TimeSeriesMessage]]("DatapointEval.out")

  override val shape: FlowShape[TimeGroup[T], List[TimeSeriesMessage]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private var state = Map.empty[StatefulExpr, Any]

      override def onPush(): Unit = {
        val group = grab(in)
        val vs = group.values
        if (vs.isEmpty) push(out, List.empty) else {
          val s = group.timestamp
          val context = EvalContext(s, s + step, step, state)
          val result = expr.expr.eval(context, vs)
          state = result.state
          val msgs = result.data.map { t =>
            TimeSeriesMessage(expr.toString, context, t)
          }
          push(out, msgs)
        }
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
