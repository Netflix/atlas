/*
 * Copyright 2014-2018 Netflix, Inc.
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
package com.netflix.atlas.eval.model

import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.DataExpr.AggregateFunction
import com.netflix.atlas.core.model.DataExpr.All
import com.netflix.atlas.core.model.DataExpr.GroupBy
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.TimeSeries

/**
  * Datapoint for an aggregate data expression. This type is used for the intermediate
  * results during evaluation of an expression until we get the final aggregated value
  * for a given query.
  *
  * @param timestamp
  *     Timestamp for all values that contributed to the aggregate. It should already
  *     be normalized to the step interval for the data stream prior to aggregation
  *     taking place.
  * @param step
  *     Step size for the subscription. Datapoints should be received at this frequency.
  * @param expr
  *     Data expression associated with the value. This is needed if further aggregation
  *     is necessary and later for matching in the final evaluation phase.
  * @param source
  *     The source combined with the expression are used for deduping the intermediate
  *     aggregates. This can be ignored at the risk of some values being included in the
  *     final result multiple times.
  * @param tags
  *     Tags associated with the datapoint.
  * @param value
  *     Value for the datapoint.
  */
case class AggrDatapoint(
  timestamp: Long,
  step: Long,
  expr: DataExpr,
  source: String,
  tags: Map[String, String],
  value: Double
) {

  /** Identifier used for deduping intermediate aggregates. */
  def id: String = s"$source:$expr"

  /**
    * Converts this value to a time series type that can be used for the final evaluation
    * phase.
    */
  def toTimeSeries: TimeSeries = Datapoint(tags, timestamp, value, step)
}

object AggrDatapoint {

  /**
    * Aggregate intermediate aggregates from each source to get the final aggregate for
    * a given expression. All values are expected to be for the same data expression.
    */
  def aggregate(values: List[AggrDatapoint]): List[AggrDatapoint] = {
    if (values.isEmpty) Nil
    else {
      val vs = dedup(values)
      val expr = vs.head.expr
      expr match {
        case af: AggregateFunction => List(applyAF(af, vs))
        case by: GroupBy           => applyGroupBy(by, vs)
        case _: All                => vs
      }
    }
  }

  /**
    * Dedup the values using the ids for each value. This will take into account the
    * group by keys such that values with different grouping keys will not be considered
    * as duplicates.
    */
  private def dedup(values: List[AggrDatapoint]): List[AggrDatapoint] = {
    values.groupBy(_.id).map(_._2.head).toList
  }

  /** Apply an aggregate function over the set of values to get the final aggregate. */
  private def applyAF(af: AggregateFunction, values: List[AggrDatapoint]): AggrDatapoint = {
    require(values.nonEmpty, "cannot apply aggregation function over empty list")
    val dp = values.head
    val value = values.map(_.value).reduce(af)
    dp.copy(value = value)
  }

  /** Apply the aggregate function for each grouping. */
  private def applyGroupBy(by: GroupBy, values: List[AggrDatapoint]): List[AggrDatapoint] = {
    val groups = values.groupBy(_.tags).map { case (_, vs) => applyAF(by.af, vs) }
    groups.toList
  }
}
