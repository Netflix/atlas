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

import com.netflix.atlas.core.model.DataExpr.AggregateFunction
import com.netflix.atlas.core.stacklang.SimpleWord
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word

object DataVocabulary extends Vocabulary {
  import com.netflix.atlas.core.model.ModelExtractors._
  import com.netflix.atlas.core.stacklang.Extractors._

  val name: String = "data"

  val dependsOn: List[Vocabulary] = List(QueryVocabulary)

  val words: List[Word] = List(
    All,
    Sum, Count, Min, Max,
    GroupBy,
    Head,
    Offset,
    CfAvg, CfSum, CfMin, CfMax
  )

  sealed trait DataWord extends SimpleWord {
    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: Query) :: _ => true
    }

    def newInstance(q: Query): DataExpr

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (q: Query) :: stack => newInstance(q) :: stack
    }

    override def signature: String = "Query -- DataExpr"

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
      """.stripMargin.trim
  }

  case object GroupBy extends SimpleWord {
    override def name: String = "by"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: List[_]) :: (_: Query) :: _             => true
      case (_: List[_]) :: (_: AggregateFunction) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (ks: List[_]) :: (q: Query) :: stack =>
        val f = DataExpr.Sum(q)
        DataExpr.GroupBy(f, ks.asInstanceOf[List[String]]) :: stack
      case (ks: List[_]) :: (f: AggregateFunction) :: stack =>
        DataExpr.GroupBy(f, ks.asInstanceOf[List[String]]) :: stack
    }

    override def summary: String =
      """
        |Compute a set of time series matching the query and grouped by the specified keys.
      """.stripMargin.trim

    override def signature: String = "af:AggregateFunction keys:List -- DataExpr"

    override def examples: List[String] = List(
      "name,sps,:eq,(,name,)",
      "name,sps,:eq,:max,(,nf.cluster,)",
      "name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,(,nf.asg,nf.zone,)")
  }

  case object Head extends SimpleWord {
    override def name: String = "head"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case IntType(_) :: DataExprType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case IntType(_) :: AggrType(af) :: stack        => af :: stack
      case IntType(n) :: DataExprType(expr) :: stack  => DataExpr.Head(expr, n) :: stack
    }

    override def summary: String =
      """
        |Restrict the output to the first `N` lines from the input expression. The lines will be
        |chosen based on a lexical ordering of the group by keys.
        |
        |Since: 1.5.0
      """.stripMargin.trim

    override def signature: String = "DataExpr Int -- DataExpr"

    override def examples: List[String] = List(
      "name,sps,:eq,4",
      "name,sps,:eq,:sum,4",
      "name,sps,:eq,:all,2",
      "name,sps,:eq,(,nf.asg,nf.zone,),:by,2",
      "ERROR:name,sps,:eq,(,nf.asg,nf.zone,),:by,0",
      "ERROR:name,sps,:eq,(,nf.asg,nf.zone,),:by,-1",
      "ERROR:42,1")
  }

  case object Offset extends SimpleWord {
    override def name: String = "offset"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case DurationType(_) :: TimeSeriesType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case DurationType(d) :: TimeSeriesType(t) :: stack => t.withOffset(d) :: stack
    }

    override def summary: String =
      """
        |Shift the time frame to use when fetching the data. This is used to look at a previous
        |interval as a point of reference, e.g., day-over-day or week-over-week.
      """.stripMargin.trim

    override def signature: String = "TimeSeriesExpr Duration -- TimeSeriesExpr"

    override def examples: List[String] = List(
      "name,sps,:eq,(,name,),:by,1w",
      "name,sps,:eq,:max,PT1H")
  }

  sealed trait CfWord extends SimpleWord {
    def cf: ConsolidationFunction
    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case AggrType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case AggrType(af) :: stack => af.withConsolidation(cf) :: stack
    }

    override def signature: String = "AggregateFunction -- DataExpr"

    override def examples: List[String] = List(
      "name,sps,:eq",
      "name,sps,:eq,:min",
      "name,sps,:eq,:max",
      "name,sps,:eq,:sum",
      "name,sps,:eq,:count")
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
