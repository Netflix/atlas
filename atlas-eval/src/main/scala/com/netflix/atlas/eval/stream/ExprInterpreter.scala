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

import org.apache.pekko.http.scaladsl.model.Uri
import com.netflix.atlas.core.model.CustomVocabulary
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Expr
import com.netflix.atlas.core.model.FilterExpr
import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.StatefulExpr
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.eval.graph.GraphConfig
import com.netflix.atlas.eval.graph.Grapher
import com.netflix.atlas.eval.graph.SimpleLegends
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.eval.util.HostRewriter
import com.typesafe.config.Config

import java.time.Duration
import scala.util.Success

private[stream] class ExprInterpreter(config: Config) {

  private val interpreter = Interpreter(new CustomVocabulary(config).allWords)

  private val grapher = Grapher(config)

  private val hostRewriter = new HostRewriter(config.getConfig("atlas.eval.host-rewrite"))

  private val maxStep = config.getDuration("atlas.eval.stream.limits.max-step")

  // Use simple legends for expressions
  private val simpleLegendsEnabled: Boolean =
    config.getBoolean("atlas.eval.graph.simple-legends-enabled")

  def eval(expr: String): List[StyleExpr] = {
    val exprs = interpreter.execute(expr).stack.map {
      case ModelExtractors.PresentationType(t) => t
      case v                                   => throw new MatchError(v)
    }
    if (simpleLegendsEnabled) SimpleLegends.generate(exprs) else exprs
  }

  def eval(uri: Uri): GraphConfig = {
    val graphCfg = grapher.toGraphConfig(uri)

    // Check step size is within bounds
    if (graphCfg.stepSize > maxStep.toMillis) {
      val step = Duration.ofMillis(graphCfg.stepSize)
      throw new IllegalArgumentException(s"max allowed step size exceeded ($step > $maxStep)")
    }

    // Check that data expressions are supported. The streaming path doesn't support
    // time shifts, filters, and integral. The filters and integral are excluded because
    // they can be confusing as the time window for evaluation is not bounded.
    val results = graphCfg.exprs.flatMap(_.perOffset)
    results.foreach { result =>
      // Use rewrite as a helper for searching the expression for invalid operations
      result.expr.rewrite {
        case op: StatefulExpr.Integral         => invalidOperator(op); op
        case op: FilterExpr                    => invalidOperator(op); op
        case op: DataExpr if !op.offset.isZero => invalidOperator(op); op
      }

      // Double check all data expressions do not have an offset. In some cases for named rewrites
      // the check above may not detect the offset.
      result.expr.dataExprs.filterNot(_.offset.isZero).foreach(invalidOperator)
    }

    // Perform host rewrites based on the Atlas hostname
    val host = uri.authority.host.toString()
    val rewritten = hostRewriter.rewrite(host, results)
    graphCfg.copy(query = rewritten.mkString(","), parsedQuery = Success(rewritten))
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

  def dataExprMap(ds: DataSources): Map[DataExpr, List[DataSource]] = {
    import scala.jdk.CollectionConverters.*
    ds.sources.asScala.toList
      .flatMap { s =>
        val exprs = eval(Uri(s.uri)).exprs.flatMap(_.expr.dataExprs).distinct
        exprs.map(_ -> s)
      }
      .groupBy(_._1)
      .map {
        case (expr, vs) => expr -> vs.map(_._2)
      }
  }
}
