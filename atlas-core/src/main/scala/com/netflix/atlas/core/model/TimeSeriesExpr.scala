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
package com.netflix.atlas.core.model

import java.time.Duration

/**
  * Base type for expressions that have a set of time series as the result.
  */
trait TimeSeriesExpr extends Expr {

  /**
    * The underlying data expressions that supply input for the evaluation. These are
    * used to fetch data from the data stores. There may be some expressions types that
    * generate data and will have an empty set. Examples are constants, random, or time.
    */
  def dataExprs: List[DataExpr]

  /**
    * Apply a time shift to all underlying data expressions.
    */
  def withOffset(d: Duration): TimeSeriesExpr = {
    val expr = rewrite {
      case e: DataExpr              => e.withOffset(d)
      case e: MathExpr.NamedRewrite => e.withOffset(d)
    }
    expr.asInstanceOf[TimeSeriesExpr]
  }

  /** Returns true if the result is grouped. See GroupBy operators. */
  def isGrouped: Boolean

  /**
    * Returns the grouping key generated for a given tag map. All keys for the group by
    * must be present in the map.
    */
  def groupByKey(tags: Map[String, String]): Option[String]

  /**
    * Returns the final grouping for the expression. For non-grouped expressions this will
    * be an empty list. If a multi-level group by is used, then this will return the grouping
    * of the final result and ignore any intermediate groupings.
    */
  def finalGrouping: List[String]

  def eval(context: EvalContext, data: List[TimeSeries]): ResultSet = {
    val rs = dataExprs.map { expr =>
      expr -> data.filter(t => expr.query.matches(t.tags))
    }
    eval(context, rs.toMap)
  }

  private def keyString(keys: List[String], tags: Map[String, String]): String = {
    val s = DataExpr.keyString(keys, tags)
    if (s == null) "" else s
  }

  private def sort(ts: List[TimeSeries], keys: List[String]): List[TimeSeries] = {
    if (keys.isEmpty) {
      ts
    } else {
      ts.sortWith { (a, b) =>
        keyString(keys, a.tags) < keyString(keys, b.tags)
      }
    }
  }

  def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
    val builder = List.newBuilder[TimeSeries]
    val evaluator = incrementalEvaluator(context)
    data.foreachEntry { (expr, ts) =>
      builder ++= sort(ts, evaluator.orderBy).flatMap(t => evaluator.update(expr, t))
    }
    builder ++= evaluator.flush()
    ResultSet(this, builder.result(), evaluator.state)
  }

  def incrementalEvaluator(context: EvalContext): IncrementalEvaluator = {
    IncrementalEvaluator(Nil)
  }
}
