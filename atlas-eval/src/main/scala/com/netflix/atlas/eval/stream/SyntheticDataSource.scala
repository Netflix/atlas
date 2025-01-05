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

import com.fasterxml.jackson.databind.JsonNode
import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.stream.IOResult
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.EventExpr
import com.netflix.atlas.core.model.Expr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.TraceQuery
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.eval.model.ExprType
import com.netflix.atlas.eval.model.LwcDataExpr
import com.netflix.atlas.eval.model.LwcDatapoint
import com.netflix.atlas.eval.model.LwcEvent
import com.netflix.atlas.eval.model.LwcSubscription
import com.netflix.atlas.eval.model.LwcSubscriptionV2
import com.netflix.atlas.json.Json
import com.netflix.spectator.impl.Hash64

import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration

/**
  * Helper for generating arbitrary data for a stream. Can be used to easily simulate
  * a lot of load without needing actual data. Just change the scheme to be `synthetic:`,
  * for example:
  *
  * ```
  * synthetic://host/api/v1/graph?q=name,sps,:eq,(,nf.cluster,),:by
  * ```
  *
  * The following URL parameters can be added to control the data volume:
  *
  * - `numStepIntervals`: how many time intervals to generate data. The source stream
  *   will stop once it has reached that amount.
  *
  * - `inputDataSize`: number of input data points to generate for each data expr.
  *
  * - `outputDataSize`: number of output data points to generate for each grouped
  *   data expr.
  */
object SyntheticDataSource {

  def apply(interpreter: ExprInterpreter, uri: Uri): Source[ByteString, Future[IOResult]] = {
    val settings = getSettings(uri)
    val (exprType, exprs) = interpreter.parseQuery(uri)
    val promise = Promise[IOResult]()
    Source(exprs)
      .flatMapMerge(Int.MaxValue, expr => source(settings, exprType, expr))
      .via(new OnUpstreamFinish[ByteString](promise.success(IOResult.createSuccessful(0L))))
      .mapMaterializedValue(_ => promise.future)
  }

  private def getSettings(uri: Uri): Settings = {
    val query = uri.query()
    Settings(
      step = query.get("step").fold(60_000L)(s => Strings.parseDuration(s).toMillis),
      numStepIntervals = query.get("numStepIntervals").fold(1440)(_.toInt),
      inputDataSize = query.get("inputDataSize").fold(1000)(_.toInt),
      outputDataSize = query.get("outputDataSize").fold(10)(_.toInt)
    )
  }

  private def source(
    settings: Settings,
    exprType: ExprType,
    expr: Expr
  ): Source[ByteString, NotUsed] = {
    exprType match {
      case ExprType.EVENTS =>
        source(settings, expr.asInstanceOf[EventExpr])
      case ExprType.TIME_SERIES =>
        source(settings, expr.asInstanceOf[StyleExpr])
      case ExprType.TRACE_EVENTS =>
        Source.empty
      case ExprType.TRACE_TIME_SERIES =>
        source(settings, expr.asInstanceOf[TraceQuery.SpanTimeSeries].expr)
    }
  }

  private def source(settings: Settings, styleExpr: StyleExpr): Source[ByteString, NotUsed] = {
    val subMessage = LwcSubscription(
      styleExpr.toString,
      styleExpr.expr.dataExprs.zipWithIndex.map {
        case (dataExpr, id) => LwcDataExpr(id.toString, dataExpr.toString, settings.step)
      }
    )

    val exprSources = styleExpr.expr.dataExprs.zipWithIndex.map {
      case (dataExpr, id) => source(settings, dataExpr, id.toString)
    }

    Source
      .single(subMessage)
      .concat(Source(exprSources).flatMapMerge(Int.MaxValue, s => s))
      .map(msg => ByteString(Json.encode(msg)))
  }

  private def source(
    settings: Settings,
    expr: DataExpr,
    id: String
  ): Source[LwcDatapoint, NotUsed] = {
    val tags = Query.tags(expr.query)
    val start = System.currentTimeMillis() / settings.step * settings.step
    Source(0 until settings.numStepIntervals)
      .throttle(1, FiniteDuration(settings.step, TimeUnit.MILLISECONDS))
      .flatMapConcat { i =>
        val timestamp = i * settings.step + start
        if (expr.isGrouped) {
          // Find the set of group by keys that are not pinned by the
          // exact tags in the query expression
          val groupByKeys = (expr.finalGrouping.toSet -- tags.keySet).toList.sorted
          val groupByDatapoint: Int => LwcDatapoint = groupByKeys match {
            case k :: ks =>
              j => {
                val v = j % settings.outputDataSize
                val ts = tags ++ ks.map(_ -> "_") + (k -> v.toString)
                LwcDatapoint(timestamp, id, ts, j.toDouble)
              }
            case Nil =>
              j => {
                LwcDatapoint(timestamp, id, tags, j.toDouble)
              }
          }
          Source(0 until settings.inputDataSize).map(groupByDatapoint)
        } else {
          Source(0 until settings.inputDataSize)
            .map { j =>
              LwcDatapoint(timestamp, id, tags, j.toDouble)
            }
        }
      }
  }

  private def source(settings: Settings, expr: EventExpr): Source[ByteString, NotUsed] = {
    val id = computeId(ExprType.EVENTS, expr, settings.step)
    val dataExpr = LwcDataExpr(id, expr.toString, settings.step)
    val subMessage = LwcSubscriptionV2(expr.toString, ExprType.EVENTS, List(dataExpr))
    val start = System.currentTimeMillis() / settings.step * settings.step

    val exprSource = Source(0 until settings.numStepIntervals)
      .throttle(1, FiniteDuration(settings.step, TimeUnit.MILLISECONDS))
      .flatMapConcat { i =>
        Source(0 until settings.inputDataSize)
          .flatMap { j =>
            val tags = Query.tags(expr.query)
            val data = Map(
              "tags" -> tags,
              "i"    -> i,
              "j"    -> j
            )
            expr match {
              case EventExpr.Raw(_) =>
                val json = Json.decode[JsonNode](Json.encode(data))
                Source.single(LwcEvent(id, json))
              case EventExpr.Table(_, cs) =>
                val columns = cs.map(c => data.getOrElse(c, null))
                val json = Json.decode[JsonNode](Json.encode(columns))
                Source.single(LwcEvent(id, json))
              case EventExpr.Sample(_, by, cs) =>
                val groupKey = groupByKey(data, by)
                if (groupKey == null) {
                  Source.empty
                } else {
                  val timestamp = i * settings.step + start
                  val columns = cs.map(c => data.getOrElse(c, null))
                  val datapoint = LwcDatapoint(
                    timestamp,
                    id,
                    tags ++ by.map(k => k -> data(k).toString),
                    1.0,
                    List(columns)
                  )
                  Source.single(datapoint)
                }
            }
          }
      }

    Source
      .single(subMessage)
      .concat(exprSource)
      .map(msg => ByteString(Json.encode(msg)))
  }

  private def groupByKey(data: Map[String, Any], by: List[String]): String = {
    if (by.forall(data.contains))
      by.map(k => data.getOrElse(k, null)).mkString(",")
    else
      null
  }

  private def computeId(exprType: ExprType, expr: Expr, step: Long): String = {
    val key = s"$exprType:$expr:$step"
    java.lang.Long.toString(new Hash64().updateString(key).compute(), 16)
  }

  case class Settings(step: Long, numStepIntervals: Int, inputDataSize: Int, outputDataSize: Int)
}
