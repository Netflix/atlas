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
package com.netflix.atlas.core.model

import java.time.Duration

trait TimeSeriesExpr extends Expr {
  def dataExprs: List[DataExpr]

  def withOffset(d: Duration): TimeSeriesExpr = {
    val expr = rewrite {
      case e: DataExpr => e.withOffset(d)
    }
    expr.asInstanceOf[TimeSeriesExpr]
  }

  def isGrouped: Boolean

  def groupByKey(tags: Map[String, String]): Option[String]

  def eval(context: EvalContext, data: List[TimeSeries]): ResultSet = {
    val rs = dataExprs.map { expr =>
      expr -> expr.eval(context, data).data
    }
    eval(context, rs.toMap)
  }

  def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet
}
