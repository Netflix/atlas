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

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.FlowShape
import org.apache.pekko.stream.Inlet
import org.apache.pekko.stream.Outlet
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.stage.GraphStage
import org.apache.pekko.stream.stage.GraphStageLogic
import org.apache.pekko.stream.stage.InHandler
import org.apache.pekko.stream.stage.OutHandler
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.StatefulExpr
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.util.IdentityMap
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.ArrayData
import com.netflix.atlas.eval.model.ChunkData
import com.netflix.atlas.eval.model.TimeGroup
import com.netflix.atlas.eval.model.TimeGroupsTuple
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.eval.stream.Evaluator.MessageEnvelope
import com.netflix.atlas.pekko.DiagnosticMessage
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable

/**
  * Takes the set of data sources and time grouped partial aggregates as input and performs
  * the final evaluation step.
  *
  * @param exprInterpreter
  *     Used for evaluating the expressions.
  * @param enableNoDataMsgs
  *     If set to true, then a no data message will be emitted for each expression if there
  *     is no data to generate an actual data point.
  */
private[stream] class FinalExprEval(exprInterpreter: ExprInterpreter, enableNoDataMsgs: Boolean)
    extends GraphStage[FlowShape[AnyRef, Source[MessageEnvelope, NotUsed]]]
    with StrictLogging {

  import FinalExprEval.*

  private val in = Inlet[AnyRef]("FinalExprEval.in")
  private val out = Outlet[Source[MessageEnvelope, NotUsed]]("FinalExprEval.out")

  override val shape: FlowShape[AnyRef, Source[MessageEnvelope, NotUsed]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {
      // Maintains the state for each expression we need to evaluate. TODO: implement
      // limits to sanity check against running of our memory
      private val states =
        scala.collection.mutable.HashMap.empty[StyleExpr, Map[StatefulExpr, Any]]

      // Step size for datapoints flowing through, it will be determined by the first data
      // sources message that arrives and should be consistent for the life of this stage
      private var step = -1L

      // Sampled event expressions
      private var sampledEventRecipients = Map.empty[DataExpr, List[ExprInfo]]

      // Each expression matched with a list of data source ids that should receive
      // the data for it
      private var recipients = List.empty[(StyleExpr, List[ExprInfo])]

      // Track the set of DataExprs per DataSource
      private var dataSourceIdToDataExprs = Map.empty[String, Set[DataExpr]]

      // Empty data map used as base to account for expressions that do not have any
      // matches for a given time interval
      private var noData = Map.empty[DataExpr, List[TimeSeries]]

      private def error(expr: String, hint: String, t: Throwable): DiagnosticMessage = {
        val str = s"$hint [[$expr]]: ${t.getClass.getSimpleName}: ${t.getMessage}"
        DiagnosticMessage.error(str)
      }

      // Updates the recipients list
      private def handleDataSources(ds: DataSources): Unit = {
        import scala.jdk.CollectionConverters.*
        val sources = ds.sources.asScala.toList
        step = ds.stepSize()

        // Get set of expressions before we update the list
        val previous = recipients.map(t => t._1 -> t._1).toMap

        // Compute set of sampled event expressions
        sampledEventRecipients = sources
          .flatMap { s =>
            exprInterpreter.parseSampleExpr(Uri(s.uri)).map { expr =>
              expr.dataExpr -> ExprInfo(s.id, None)
            }
          }
          .groupBy(_._1)
          .map(t => t._1 -> t._2.map(_._2))

        // Error messages for invalid expressions
        val errors = List.newBuilder[MessageEnvelope]

        // Compute the new set of expressions
        recipients = sources
          .flatMap { s =>
            try {
              exprInterpreter.evalTimeSeries(Uri(s.uri)).toList.flatMap { graphCfg =>
                val exprs = graphCfg.exprs
                // Reuse the previous evaluated expression if available. States for the stateful
                // expressions are maintained in an IdentityHashMap so if the instances change
                // the state will be reset.
                exprs.map { e =>
                  val paletteName =
                    if (graphCfg.flags.presentationMetadataEnabled) {
                      val axis = e.axis.getOrElse(0)
                      Some(graphCfg.flags.axisPalette(graphCfg.settings, axis))
                    } else {
                      None
                    }
                  previous.getOrElse(e, e) -> ExprInfo(s.id, paletteName)
                }
              }
            } catch {
              case e: Exception =>
                errors += new MessageEnvelope(s.id, error(s.uri, "invalid expression", e))
                Nil
            }
          }
          .groupBy(_._1)
          .map(t => t._1 -> t._2.map(_._2))
          .toList

        dataSourceIdToDataExprs = recipients
          .flatMap(styleExprAndIds =>
            styleExprAndIds._2.map(id => id -> styleExprAndIds._1.expr.dataExprs.toSet)
          )
          // Fold to mutable map to avoid creating new Map on every update
          .foldLeft(mutable.Map.empty[String, Set[DataExpr]]) {
            case (map, (info, dataExprs)) =>
              map += map.get(info.id).fold(info.id -> dataExprs) { vs =>
                info.id -> (dataExprs ++ vs)
              }
          }
          .toMap

        // Cleanup state for any expressions that are no longer needed
        val removed = previous.keySet -- recipients.map(_._1).toSet
        removed.foreach { expr =>
          states -= expr
        }

        // Setup no data map
        noData = recipients
          .flatMap(_._1.expr.dataExprs)
          .distinct
          .map {
            // If there is no grouping, then use a no data line, otherwise use an empty set
            case e if e.finalGrouping.isEmpty =>
              e -> List(TimeSeries.noData(e.query, step))
            case e =>
              e -> Nil
          }
          .toMap

        push(out, Source(errors.result()))
      }

      // Generate a no data line for a full expression. Use the tagging information from the
      // first data expression that is found.
      private def noData(expr: StyleExpr): TimeSeries = {
        expr.expr.dataExprs.headOption match {
          case Some(e) => TimeSeries.noData(e.query, step)
          case None    => TimeSeries.noData(step)
        }
      }

      // Perform the final evaluation and create a source with the TimeSeriesMessages
      // addressed to each recipient
      private def handleData(group: TimeGroup): List[MessageEnvelope] = {
        // Finalize the DataExprs, needed as input for further evaluation
        val timestamp = group.timestamp
        val groupedDatapoints = group.dataExprValues

        // Messages for sampled events that look similar to time series
        val sampledEventMessages = groupedDatapoints.flatMap {
          case (expr, vs) =>
            sampledEventRecipients
              .get(expr)
              .fold(List.empty[MessageEnvelope]) { infos =>
                val ts = vs.values.map(toTimeSeriesMessage)
                ts.flatMap { msg =>
                  infos.map { info =>
                    new MessageEnvelope(info.id, msg)
                  }
                }
              }
        }.toList

        // Data for each time series data expression or no-data line if there is no data for
        // the interval
        val dataExprToDatapoints = noData ++ groupedDatapoints.map {
          case (k, vs) =>
            k -> vs.values.map(_.toTimeSeries)
        }

        // Collect input and intermediate data size per DataSource
        val rateCollector = new EvalDataRateCollector(timestamp, step)
        dataSourceIdToDataExprs.foreach {
          case (id, dataExprSet) =>
            dataExprSet.foreach { dataExpr =>
              group.dataExprValues.get(dataExpr).foreach { info =>
                rateCollector.incrementInput(id, dataExpr, info.numRawDatapoints)
                rateCollector.incrementIntermediate(id, dataExpr, info.values.size)
              }
            }
        }

        // Generate the time series and diagnostic output
        val output = recipients
          .flatMap {
            case (styleExpr, infos) =>
              val exprStr = styleExpr.toString
              val ids = infos.map(_.id)
              // Use an identity map for the state to ensure that multiple equivalent stateful
              // expressions, e.g. derivative(a) + derivative(a), will have isolated state.
              val state = states.getOrElse(styleExpr, IdentityMap.empty[StatefulExpr, Any])
              val context = EvalContext(timestamp, timestamp + step, step, state)
              try {
                val result = styleExpr.expr.eval(context, dataExprToDatapoints)
                states(styleExpr) = result.state
                val data = if (result.data.isEmpty) List(noData(styleExpr)) else result.data

                // Collect final data size per DataSource
                ids.foreach(rateCollector.incrementOutput(_, data.size))

                // Create time series messages
                infos.flatMap { info =>
                  data.map { t =>
                    val ts = TimeSeriesMessage(
                      styleExpr,
                      context,
                      t.withLabel(styleExpr.legend(t)),
                      info.palette,
                      Some(exprStr)
                    )
                    new MessageEnvelope(info.id, ts)
                  }
                }
              } catch {
                case e: Exception =>
                  val msg = error(styleExpr.toString, "final eval failed", e)
                  ids.map { id =>
                    new MessageEnvelope(id, msg)
                  }
              }
          }
          .filter { env =>
            enableNoDataMsgs || hasFiniteValue(env.message())
          }

        val rateMessages = rateCollector.getAll.map {
          case (id, rate) => new MessageEnvelope(id, rate)
        }.toList

        sampledEventMessages ++ output ++ rateMessages
      }

      private def hasFiniteValue(value: AnyRef): Boolean = {
        value match {
          case ts: TimeSeriesMessage => valueNotNaN(ts.data)
          case _                     => true
        }
      }

      private def valueNotNaN(value: ChunkData): Boolean = {
        value match {
          case ArrayData(vs) => vs.exists(!_.isNaN)
          case null          => true
        }
      }

      private def toTimeSeriesMessage(value: AggrDatapoint): TimeSeriesMessage = {
        // For sampled messages, convert to rate per step, i.e. a raw count per step, rather
        // than a rate per second.
        val secondsPerStep = step / 1000.0
        val ratePerStep = value.value * secondsPerStep
        val id = TaggedItem.computeId(value.tags + ("atlas.query" -> value.source)).toString
        TimeSeriesMessage(
          id,
          value.source,
          value.expr.finalGrouping,
          value.timestamp,
          value.timestamp + step,
          step,
          TimeSeries.toLabel(value.tags),
          value.tags,
          ArrayData(Array(ratePerStep)),
          None,
          value.samples
        )
      }

      private def handleSingleGroup(g: TimeGroup): Unit = {
        push(out, Source(handleData(g)))
      }

      private def handleGroups(t: TimeGroupsTuple): Unit = {
        val msgs = List.newBuilder[MessageEnvelope]
        msgs ++= t.messages
        msgs ++= t.groups.flatMap(handleData)
        push(out, Source(msgs.result()))
      }

      override def onPush(): Unit = {
        grab(in) match {
          case ds: DataSources    => handleDataSources(ds)
          case data: TimeGroup    => handleSingleGroup(data)
          case t: TimeGroupsTuple => handleGroups(t)
          case v                  => throw new MatchError(v)
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

object FinalExprEval {

  case class ExprInfo(id: String, palette: Option[String])
}
