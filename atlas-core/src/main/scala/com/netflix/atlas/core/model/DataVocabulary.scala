/*
 * Copyright 2014-2026 Netflix, Inc.
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

import scala.collection.immutable.ArraySeq

import com.netflix.atlas.core.model.DataExpr.AggregateFunction
import com.netflix.atlas.core.model.MathExpr.NamedRewrite
import com.netflix.atlas.core.stacklang.Context
import com.netflix.atlas.core.stacklang.TypedWord
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word
import com.netflix.atlas.core.stacklang.ast.DataType
import com.netflix.atlas.core.stacklang.ast.Parameter

object DataVocabulary extends Vocabulary {

  import com.netflix.atlas.core.model.ModelDataTypes.*

  val name: String = "data"

  val dependsOn: List[Vocabulary] = List(QueryVocabulary)

  val words: List[Word] = List(
    All,
    Sum,
    Count,
    Min,
    Max,
    GroupBy,
    Offset,
    CfAvg,
    CfSum,
    CfMin,
    CfMax
  )

  sealed trait DataWord extends TypedWord {

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("q", "input query", QueryType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(DataExprType)

    def newInstance(q: Query): DataExpr

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val q = params(0).asInstanceOf[Query]
      context.copy(stack = newInstance(q) :: context.stack)
    }

    override def examples: List[String] = List("name,sps,:eq")
  }

  case object All extends DataWord {

    override def name: String = "all"

    def newInstance(q: Query): DataExpr = DataExpr.All(q)

    override def summary: String =
      """
        |Fetch all time series that match the query.
        |
        |> :warning: This operation is primarily intended for debugging and can have strange
        |behaviour when used with rollups. Most users should use [:by](data-by) instead.
      """.stripMargin.trim

    override def examples: List[String] = List("name,sps,:eq,nf.cluster,nccp-ps3,:eq,:and")
  }

  case object Sum extends DataWord {

    override def name: String = "sum"

    def newInstance(q: Query): DataExpr = DataExpr.Sum(q)

    override def summary: String =
      """
        |Compute the sum of all the time series that match the query. Sum is the default aggregate
        |used if a query is specified with no explicit aggregate function.
        |
        || Sum     | 3.0 | 4.0 | NaN |
        ||---------|-----|-----|-----|
        || Input 1 | 2.0 | 4.0 | NaN |
        || Input 2 | 1.0 | NaN | NaN |
        |
        |To compute sum of multiple time series that result from a [group by](data-by) expression,
        |refer to [math/:sum](math-sum) instead.
      """.stripMargin.trim
  }

  case object Count extends DataWord {

    override def name: String = "count"

    def newInstance(q: Query): DataExpr = DataExpr.Count(q)

    override def summary: String =
      """
        |Compute the number of time series that match the query and have a value for a given
        |interval.
        |
        || Count   | 2.0 | 1.0 | NaN |
        ||---------|-----|-----|-----|
        || Input 1 | 2.0 | 4.0 | NaN |
        || Input 2 | 1.0 | NaN | NaN |
        |
        |To compute count of multiple time series that result from a [group by](data-by) expression,
        |refer to [math/:count](math-count) instead.
      """.stripMargin.trim
  }

  case object Min extends DataWord {

    override def name: String = "min"

    def newInstance(q: Query): DataExpr = DataExpr.Min(q)

    override def summary: String =
      """
        |For each interval compute the min of the values from all the time series that match
        |the query.
        |
        || Min     | 1.0 | 4.0 | NaN |
        ||---------|-----|-----|-----|
        || Input 1 | 2.0 | 4.0 | NaN |
        || Input 2 | 1.0 | NaN | NaN |
        |
        |To compute min of multiple time series that result from a [group by](data-by) expression,
        |refer to [math/:min](math-min) instead.
      """.stripMargin.trim
  }

  case object Max extends DataWord {

    override def name: String = "max"

    def newInstance(q: Query): DataExpr = DataExpr.Max(q)

    override def summary: String =
      """
        |For each interval compute the max of the values from all the time series that match
        |the query.
        |
        || Max     | 2.0 | 4.0 | NaN |
        ||---------|-----|-----|-----|
        || Input 1 | 2.0 | 4.0 | NaN |
        || Input 2 | 1.0 | NaN | NaN |
        |
        |To compute max of multiple time series that result from a [group by](data-by) expression,
        |refer to [math/:max](math-max) instead.
      """.stripMargin.trim
  }

  case object GroupBy extends TypedWord {

    override def name: String = "by"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("af", "aggregate function or query", AggrType),
      Parameter("keys", "tag keys to group by", DataType.StringListType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(DataExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val af = params(0).asInstanceOf[AggregateFunction]
      val keys = params(1).asInstanceOf[List[String]]
      context.copy(stack = DataExpr.GroupBy(af, keys) :: context.stack)
    }

    override def summary: String =
      """
        |Compute a set of time series matching the query and grouped by the specified keys.
      """.stripMargin.trim

    override def examples: List[String] =
      List(
        "name,sps,:eq,(,name,)",
        "name,sps,:eq,:max,(,nf.cluster,)",
        "name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,(,nf.asg,nf.zone,)"
      )
  }

  case object Offset extends TypedWord with StylePassthrough {

    override def name: String = "offset"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("", "input time series", TimeSeriesExprType),
      Parameter("dur", "offset duration", DataType.DurationType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val t = params(0).asInstanceOf[TimeSeriesExpr]
      val d = params(1).asInstanceOf[java.time.Duration]
      context.copy(stack = t.withOffset(d) :: context.stack)
    }

    override def summary: String =
      """
        |Shift the time frame to use when fetching the data. This is used to look at a previous
        |interval as a point of reference, e.g., day-over-day or week-over-week.
      """.stripMargin.trim

    override def examples: List[String] =
      List("name,sps,:eq,(,name,),:by,1w", "name,sps,:eq,:max,PT1H")
  }

  sealed trait CfWord extends TypedWord {

    def cf: ConsolidationFunction

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("expr", "input expression", TimeSeriesExprType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(DataExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val t = params(0).asInstanceOf[TimeSeriesExpr]
      // Expand rewrites, custom consolidation cannot be preserved with the rewrite in
      // place. Expand so we can preserve correctness.
      val evalExpr = t.rewrite {
        case nr: NamedRewrite => nr.evalExpr
      }
      // Update the aggregation functions within the expression to use the specified
      // consolidation function
      val expr = evalExpr.rewrite {
        case af: AggregateFunction => af.withConsolidation(cf)
      }
      context.copy(stack = expr :: context.stack)
    }

    override def examples: List[String] =
      List(
        "name,sps,:eq",
        "name,sps,:eq,:min",
        "name,sps,:eq,:max",
        "name,sps,:eq,:sum",
        "name,sps,:eq,:count"
      )
  }

  case object CfSum extends CfWord {

    override def cf: ConsolidationFunction = ConsolidationFunction.Sum

    override def name: String = "cf-sum"

    override def summary: String =
      """
        |Consolidate using the sum of the primary datapoints.
      """.stripMargin.trim
  }

  case object CfAvg extends CfWord {

    override def cf: ConsolidationFunction = ConsolidationFunction.Avg

    override def name: String = "cf-avg"

    override def summary: String =
      """
        |Consolidate using the average of the primary datapoints.
      """.stripMargin.trim
  }

  case object CfMin extends CfWord {

    override def cf: ConsolidationFunction = ConsolidationFunction.Min

    override def name: String = "cf-min"

    override def summary: String =
      """
        |Consolidate using the minimum of the primary datapoints.
      """.stripMargin.trim
  }

  case object CfMax extends CfWord {

    override def cf: ConsolidationFunction = ConsolidationFunction.Max

    override def name: String = "cf-max"

    override def summary: String =
      """
        |Consolidate using the maximum of the primary datapoints.
      """.stripMargin.trim
  }
}
