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
package com.netflix.atlas.eval.graph

import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.MathExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.Query.KeyValueQuery
import com.netflix.atlas.core.model.StyleExpr
import com.typesafe.scalalogging.StrictLogging

/**
  * Helper to analyze a set of expressions and try to automatically set a reasonable
  * human readable legend.
  */
object SimpleLegends extends StrictLogging {

  def generate(exprs: List[StyleExpr]): List[StyleExpr] = {
    try {
      // Extract key/value pairs from all expressions
      val kvs = exprs
        .map(e => e -> extractKeyValues(e))
        .filterNot(_._2.isEmpty)
        .toMap

      if (kvs.isEmpty) {
        exprs
      } else {
        // Figure out the unique set
        val common = kvs.values.reduce(intersect)
        exprs.map { expr =>
          if (hasExplicitLegend(expr)) {
            expr
          } else if (kvs.contains(expr)) {
            val kv = kvs(expr)
            val uniq = diff(kv, common)
            if (uniq.nonEmpty)
              generateLegend(expr, uniq)
            else if (common.nonEmpty)
              generateLegend(expr, common)
            else
              expr
          } else {
            expr
          }
        }
      }
    } catch {
      // This is a nice to have presentation detail. In case it fails for any reason we
      // just fallback to the defaults. Under normal usage this is not expected to ever
      // be reached.
      case e: Exception =>
        logger.warn("failed to generate simple legend, using default", e)
        exprs
    }
  }

  private def hasExplicitLegend(expr: StyleExpr): Boolean = {
    expr.settings.contains("legend")
  }

  private def withLegend(expr: StyleExpr, legend: String): StyleExpr = {
    val label = if (expr.offset > 0L) s"$legend (offset=$$(atlas.offset))" else legend
    expr.copy(settings = expr.settings + ("legend" -> label))
  }

  private def keyValues(query: Query): Map[String, String] = {
    query match {
      case Query.And(q1, q2)            => keyValues(q1) ++ keyValues(q2)
      case Query.Equal(k, v)            => Map(k -> v)
      case Query.LessThan(k, v)         => Map(k -> v)
      case Query.LessThanEqual(k, v)    => Map(k -> v)
      case Query.GreaterThan(k, v)      => Map(k -> v)
      case Query.GreaterThanEqual(k, v) => Map(k -> v)
      case re: Query.Regex              => Map(re.k -> regexPresentationValue(re))
      case Query.RegexIgnoreCase(k, v)  => Map(k -> v)
      case Query.Not(q: KeyValueQuery)  => keyValues(q).map(t => t._1 -> s"!${t._2}")
      case _                            => Map.empty
    }
  }

  private def regexPresentationValue(re: Query.Regex): String = {
    if (re.pattern.isPrefixMatcher)
      re.pattern.prefix()
    else if (re.pattern.isContainsMatcher)
      re.pattern.containedString()
    else
      re.v
  }

  private def generateLegend(expr: StyleExpr, kv: Map[String, String]): StyleExpr = {
    if (expr.expr.isGrouped) {
      val fmt = expr.expr.finalGrouping.mkString("$(", ") $(", ")")
      withLegend(expr, fmt)
    } else if (kv.contains("name")) {
      withLegend(expr, kv("name"))
    } else {
      val legend = kv.toList.sortWith(_._1 < _._1).map(_._2).mkString(" ")
      withLegend(expr, legend)
    }
  }

  private def extractKeyValues(expr: StyleExpr): Map[String, String] = {
    val dataExprs = removeNamedRewrites(expr).expr.dataExprs
    if (dataExprs.isEmpty)
      Map.empty
    else
      dataExprs.map(de => keyValues(de.query)).reduce(intersect)
  }

  private def removeNamedRewrites(expr: StyleExpr): StyleExpr = {
    // Custom averages like dist-avg and node-avg are done with rewrites that can
    // lead to confusing legends. For the purposes here those can be rewritten to
    // a simple aggregate like sum based on the display expression.
    expr
      .rewrite {
        case MathExpr.NamedRewrite(n, q: Query, _, evalExpr, _, _) if n.endsWith("-avg") =>
          val aggr = DataExpr.Sum(q)
          if (evalExpr.isGrouped) DataExpr.GroupBy(aggr, evalExpr.finalGrouping) else aggr
      }
      .asInstanceOf[StyleExpr]
  }

  private def intersect(m1: Map[String, String], m2: Map[String, String]): Map[String, String] = {
    m1.toSet.intersect(m2.toSet).toMap
  }

  private def diff(m1: Map[String, String], m2: Map[String, String]): Map[String, String] = {
    m1.toSet.diff(m2.toSet).toMap
  }
}
