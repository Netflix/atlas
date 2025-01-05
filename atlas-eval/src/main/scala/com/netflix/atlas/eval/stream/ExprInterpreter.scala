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

import com.netflix.atlas.core.model.CustomVocabulary
import org.apache.pekko.http.scaladsl.model.Uri
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.EventExpr
import com.netflix.atlas.core.model.EventVocabulary
import com.netflix.atlas.core.model.Expr
import com.netflix.atlas.core.model.FilterExpr
import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.StatefulExpr
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.TraceQuery
import com.netflix.atlas.core.model.TraceVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.eval.graph.GraphConfig
import com.netflix.atlas.eval.graph.Grapher
import com.netflix.atlas.eval.model.ExprType
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.eval.util.HostRewriter
import com.typesafe.config.Config

import scala.util.Success

class ExprInterpreter(config: Config) {

  import ExprInterpreter.*

  private val tsInterpreter = Interpreter(new CustomVocabulary(config).allWords)

  private val eventInterpreter = Interpreter(
    new CustomVocabulary(config, List(EventVocabulary)).allWords
  )

  private val traceInterpreter = Interpreter(
    new CustomVocabulary(config, List(TraceVocabulary)).allWords
  )

  private val grapher = Grapher(config)

  private val hostRewriter = new HostRewriter(config.getConfig("atlas.eval.host-rewrite"))

  /** Evaluate a normal Atlas Graph URI and produce and graph config. */
  def eval(uri: Uri): GraphConfig = {
    val graphCfg = grapher.toGraphConfig(uri)

    // Check that data expressions are supported. The streaming path doesn't support
    // time shifts, filters, and integral. The filters and integral are excluded because
    // they can be confusing as the time window for evaluation is not bounded.
    val results = graphCfg.exprs.flatMap(_.perOffset)
    results.foreach(validate)

    // Perform host rewrites based on the Atlas hostname
    val host = uri.authority.host.toString()
    val rewritten = hostRewriter.rewrite(host, results)
    graphCfg.copy(query = rewritten.mkString(","), parsedQuery = Success(rewritten))
  }

  /**
    * Evaluate a time series URI. This could be an normal Atlas Graph URI or a trace
    * time series URI. If a non-time series URI is passed in the result will be None.
    */
  def evalTimeSeries(uri: Uri): Option[GraphConfig] = {
    determineExprType(uri) match {
      case ExprType.TIME_SERIES       => Some(eval(uri))
      case ExprType.TRACE_TIME_SERIES => Some(eval(toGraphUri(uri)))
      case _                          => None
    }
  }

  /** Convert a trace time series URI to a normal graph URI for generating the config. */
  private def toGraphUri(uri: Uri): Uri = {
    val ts = evalTraceTimeSeries(uri)
    val newExpr = ts.map(_.expr).mkString(",")
    val newQuery = uri.query().filterNot(_._1 == "q").prepended("q" -> newExpr)
    uri.withQuery(Uri.Query(newQuery*))
  }

  /**
    * Check that data expressions are supported. The streaming path doesn't support
    * time shifts, filters, and integral. The filters and integral are excluded because
    * they can be confusing as the time window for evaluation is not bounded.
    */
  private def validate(styleExpr: StyleExpr): Unit = {
    styleExpr.perOffset.foreach { s =>
      // Use rewrite as a helper for searching the expression for invalid operations
      s.expr.rewrite {
        case op: StatefulExpr.Integral         => invalidOperator(op); op
        case op: FilterExpr                    => invalidOperator(op); op
        case op: DataExpr if !op.offset.isZero => invalidOperator(op); op
      }

      // Double check all data expressions do not have an offset. In some cases for named rewrites
      // the check above may not detect the offset.
      s.expr.dataExprs.filterNot(_.offset.isZero).foreach(invalidOperator)
    }
  }

  private def invalidOperator(expr: Expr): Unit = {
    // The invalid operation should be at the end of the expression string so it
    // can be easily extracted.
    val s = expr.toString
    val i = s.lastIndexOf(':'.toInt)
    val op = if (i >= 0) s.substring(i) else "unknown"
    throw new IllegalArgumentException(
      s"$op not supported for streaming evaluation [[$expr]]"
    )
  }

  def dataExprMap(ds: DataSources): Map[String, List[DataSource]] = {
    import scala.jdk.CollectionConverters.*
    ds.sources.asScala.toList
      .flatMap { s =>
        dataExprs(Uri(s.uri)).map(_ -> s)
      }
      .groupBy(_._1)
      .map {
        case (expr, vs) => expr -> vs.map(_._2)
      }
  }

  private def invalidValue(value: Any): IllegalArgumentException = {
    new IllegalArgumentException(s"invalid value on stack: $value")
  }

  private def parseTimeSeries(query: String): List[StyleExpr] = {
    val exprs = tsInterpreter.execute(query).stack.map {
      case ModelExtractors.PresentationType(t) => t
      case value                               => throw invalidValue(value)
    }
    exprs.foreach(validate)
    exprs
  }

  private def parseEvents(query: String): List[EventExpr] = {
    eventInterpreter.execute(query).stack.map {
      case ModelExtractors.EventExprType(t) => t
      case value                            => throw invalidValue(value)
    }
  }

  private def evalEvents(uri: Uri): List[EventExpr] = {
    uri.query().get("q") match {
      case Some(query) =>
        parseEvents(query)
      case None =>
        throw new IllegalArgumentException(s"missing required parameter: q ($uri)")
    }
  }

  private def parseTraceEvents(query: String): List[TraceQuery.SpanFilter] = {
    traceInterpreter.execute(query).stack.map {
      case ModelExtractors.TraceFilterType(t) => t
      case value                              => throw invalidValue(value)
    }
  }

  private def evalTraceEvents(uri: Uri): List[TraceQuery.SpanFilter] = {
    uri.query().get("q") match {
      case Some(query) =>
        parseTraceEvents(query)
      case None =>
        throw new IllegalArgumentException(s"missing required parameter: q ($uri)")
    }
  }

  private def parseTraceTimeSeries(query: String): List[TraceQuery.SpanTimeSeries] = {
    val exprs = traceInterpreter.execute(query).stack.map {
      case ModelExtractors.TraceTimeSeriesType(t) => t
      case value                                  => throw invalidValue(value)
    }
    exprs.foreach(t => validate(t.expr))
    exprs
  }

  private def evalTraceTimeSeries(uri: Uri): List[TraceQuery.SpanTimeSeries] = {
    uri.query().get("q") match {
      case Some(query) =>
        parseTraceTimeSeries(query)
      case None =>
        throw new IllegalArgumentException(s"missing required parameter: q ($uri)")
    }
  }

  /** Parse an expression based on the type. */
  def parseQuery(expr: String, exprType: ExprType): List[Expr] = {
    val exprs = exprType match {
      case ExprType.TIME_SERIES       => parseTimeSeries(expr)
      case ExprType.EVENTS            => parseEvents(expr)
      case ExprType.TRACE_EVENTS      => parseTraceEvents(expr)
      case ExprType.TRACE_TIME_SERIES => parseTraceTimeSeries(expr)
    }
    exprs.distinct
  }

  /** Parse an expression that is part of a URI. */
  def parseQuery(uri: Uri): (ExprType, List[Expr]) = {
    val exprType = determineExprType(uri)
    val exprs = exprType match {
      case ExprType.TIME_SERIES       => eval(uri).exprs
      case ExprType.EVENTS            => evalEvents(uri)
      case ExprType.TRACE_EVENTS      => evalTraceEvents(uri)
      case ExprType.TRACE_TIME_SERIES => evalTraceTimeSeries(uri)
    }
    exprType -> exprs.distinct
  }

  /** Parse sampled event expression URI. */
  def parseSampleExpr(uri: Uri): List[EventExpr.Sample] = {
    determineExprType(uri) match {
      case ExprType.EVENTS => evalEvents(uri).collect { case s: EventExpr.Sample => s }
      case _               => Nil
    }
  }

  def dataExprs(uri: Uri): List[String] = {
    val exprs = determineExprType(uri) match {
      case ExprType.TIME_SERIES       => eval(uri).exprs.flatMap(_.expr.dataExprs)
      case ExprType.EVENTS            => evalEvents(uri)
      case ExprType.TRACE_EVENTS      => evalTraceEvents(uri)
      case ExprType.TRACE_TIME_SERIES => evalTraceTimeSeries(uri)
    }
    exprs.map(_.toString).distinct
  }

  def determineExprType(uri: Uri): ExprType = {
    val reversed = reversedPath(uri.path)
    if (reversed.startsWith(eventsPrefix))
      ExprType.EVENTS
    else if (reversed.startsWith(traceEventsPrefix))
      ExprType.TRACE_EVENTS
    else if (reversed.startsWith(traceTimeSeriesPrefix))
      ExprType.TRACE_TIME_SERIES
    else
      ExprType.TIME_SERIES
  }

  private def reversedPath(path: Uri.Path): Uri.Path = {
    val reversed = path.reverse
    if (reversed.startsWithSlash)
      reversed.tail
    else
      reversed
  }
}

private[stream] object ExprInterpreter {

  private val eventsPrefix = Uri.Path("events")
  private val traceEventsPrefix = Uri.Path("traces")
  private val traceTimeSeriesPrefix = Uri.Path("graph") / "traces"
}
