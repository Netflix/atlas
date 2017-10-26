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

import akka.NotUsed
import akka.http.scaladsl.model.Uri
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.StatefulExpr
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.TimeGroup
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.eval.stream.Evaluator.MessageEnvelope
import com.typesafe.scalalogging.StrictLogging

/**
  * Takes the set of data sources and time grouped partial aggregates as input and performs
  * the final evaluation step.
  *
  * @param step
  *     Step size for the input data.
  */
private[stream] class FinalExprEval(step: Long = 60000L)
    extends GraphStage[FlowShape[AnyRef, Source[MessageEnvelope, NotUsed]]]
    with StrictLogging {

  private val in = Inlet[AnyRef]("FinalExprEval.in")
  private val out = Outlet[Source[MessageEnvelope, NotUsed]]("FinalExprEval.out")

  override val shape: FlowShape[AnyRef, Source[MessageEnvelope, NotUsed]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {
      // Maintains the state for each expression we need to evaluate. TODO: implement
      // limits to sanity check against running of our memory
      private val states =
        scala.collection.mutable.AnyRefMap.empty[StyleExpr, Map[StatefulExpr, Any]]

      // Each expression matched with a list of data source ids that should receive
      // the data for it
      private var recipients = List.empty[(StyleExpr, List[String])]

      // Empty data map used as base to account for expressions that do not have any
      // matches for a given time interval
      private var noData = Map.empty[DataExpr, List[TimeSeries]]

      private def error(expr: String, hint: String, t: Throwable): DiagnosticMessage = {
        val str = s"$hint [[$expr]]: ${t.getClass.getSimpleName}: ${t.getMessage}"
        DiagnosticMessage.error(str)
      }

      // Updates the recipients list
      private def handleDataSources(ds: DataSources): Unit = {
        import scala.collection.JavaConverters._
        val sources = ds.getSources.asScala

        // Get set of expressions before we update the list
        val previous = recipients.map(_._1).toSet

        // Error messages for invalid expressions
        val errors = List.newBuilder[MessageEnvelope]

        // Compute the new set of expressions
        recipients = sources
          .flatMap { s =>
            try {
              val exprs = ExprInterpreter.eval(Uri(s.getUri))
              exprs.map(e => e -> s.getId)
            } catch {
              case e: Exception =>
                errors += new MessageEnvelope(s.getId, error(s.getUri, "invalid expression", e))
                Nil
            }
          }
          .groupBy(_._1)
          .map(t => t._1 -> t._2.map(_._2).toList)
          .toList

        // Cleanup state for any expressions that are no longer needed
        val removed = previous -- recipients.map(_._1).toSet
        removed.foreach { expr =>
          states -= expr
        }

        // Setup no data map
        noData = recipients
          .flatMap(_._1.expr.dataExprs)
          .distinct
          .map(e => e -> List(TimeSeries.noData(e.query, step)))
          .toMap

        push(out, Source(errors.result()))
      }

      // Perform the final evaluation and create a source with the TimeSeriesMessages
      // addressed to each recipient
      private def handleData(group: TimeGroup[AggrDatapoint]): Unit = {
        // Finalize the DataExprs, needed as input for further evaluation
        val (exprDataBldr, exprDiagBldr) = group.values.groupBy(_.expr).
          foldLeft(Map.newBuilder[DataExpr, List[TimeSeries]], Map.newBuilder[DataExpr, DiagnosticMessage]) {
            case ((dataBldr, diagBldr), (dataExpr, datapoints)) =>
              val timeseries = AggrDatapoint.aggregate(datapoints).map(_.toTimeSeries(step))
              val diagnosticMessage = DiagnosticMessage.info(s"Stats for expression: [$dataExpr]. " +
                s"Input of ${datapoints.length} datapoints further aggregated to ${timeseries.length} datapoints.")

              dataBldr += dataExpr -> timeseries
              diagBldr += dataExpr -> diagnosticMessage

              (dataBldr, diagBldr)
          }

        val expressionDatapoints = noData ++ exprDataBldr.result()
        val expressionDiagnostics = exprDiagBldr.result()

        val s = group.timestamp

        // Generate the time series and diagnostic output
        val output = recipients.flatMap {
          case (expr, ids) =>
            val state = states.getOrElse(expr, Map.empty[StatefulExpr, Any])
            val context = EvalContext(s, s + step, step, state)
            try {
              val result = expr.expr.eval(context, expressionDatapoints)
              states(expr) = result.state
              val msgs = result.data.map { t =>
                TimeSeriesMessage(expr.toString, context, t)
              }

              val diagnostics = expr.expr.dataExprs.flatMap(expressionDiagnostics.get)

              ids.flatMap { id =>
                msgs.map { msg =>
                  new MessageEnvelope(id, msg)
                } ++
                diagnostics.map { diag =>
                  new MessageEnvelope(id, diag)
                }
              }
            } catch {
              case e: Exception =>
                val msg = error(expr.toString, "final eval failed", e)
                ids.map { id =>
                  new MessageEnvelope(id, msg)
                }
            }
        }

        push(out, Source(output))
      }

      override def onPush(): Unit = {
        grab(in) match {
          case ds: DataSources    => handleDataSources(ds)
          case data: TimeGroup[_] => handleData(data.asInstanceOf[TimeGroup[AggrDatapoint]])
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
