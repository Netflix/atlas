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

import java.time.Duration

import scala.collection.immutable.ArraySeq

import com.netflix.atlas.core.stacklang.Context
import com.netflix.atlas.core.stacklang.TypedMacro
import com.netflix.atlas.core.stacklang.TypedWord
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word
import com.netflix.atlas.core.stacklang.ast.DataType
import com.netflix.atlas.core.stacklang.ast.Parameter

object FilterVocabulary extends Vocabulary {

  import com.netflix.atlas.core.model.ModelDataTypes.*

  val name: String = "filter"

  val dependsOn: List[Vocabulary] = List(StatefulVocabulary)

  val words: List[Word] = List(
    Consolidate,
    Stat,
    StatAvg,
    StatMax,
    StatMin,
    StatLast,
    StatCount,
    StatTotal,
    Filter,
    // Legacy operations equivalent to `max,:stat`
    TypedMacro(
      "stat-min-mf",
      List("min", ":stat"),
      ArraySeq(Parameter("", "input time series", TimeSeriesExprType)),
      ArraySeq(TimeSeriesExprType),
      "Compute min summary statistic. Shorthand for `min,:stat`.",
      List("42")
    ),
    TypedMacro(
      "stat-max-mf",
      List("max", ":stat"),
      ArraySeq(Parameter("", "input time series", TimeSeriesExprType)),
      ArraySeq(TimeSeriesExprType),
      "Compute max summary statistic. Shorthand for `max,:stat`.",
      List("42")
    ),
    TypedMacro(
      "stat-avg-mf",
      List("avg", ":stat"),
      ArraySeq(Parameter("", "input time series", TimeSeriesExprType)),
      ArraySeq(TimeSeriesExprType),
      "Compute avg summary statistic. Shorthand for `avg,:stat`.",
      List("42")
    ),
    // Priority operators: https://github.com/Netflix/atlas/issues/1224
    PriorityK("bottomk", FilterExpr.BottomK.apply),
    PriorityK("bottomk-others-min", FilterExpr.BottomKOthersMin.apply),
    PriorityK("bottomk-others-max", FilterExpr.BottomKOthersMax.apply),
    PriorityK("bottomk-others-sum", FilterExpr.BottomKOthersSum.apply),
    PriorityK("bottomk-others-avg", FilterExpr.BottomKOthersAvg.apply),
    PriorityK("topk", FilterExpr.TopK.apply),
    PriorityK("topk-others-min", FilterExpr.TopKOthersMin.apply),
    PriorityK("topk-others-max", FilterExpr.TopKOthersMax.apply),
    PriorityK("topk-others-sum", FilterExpr.TopKOthersSum.apply),
    PriorityK("topk-others-avg", FilterExpr.TopKOthersAvg.apply)
  )

  case object Stat extends TypedWord with StylePassthrough {

    override def name: String = "stat"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("", "input time series", TimeSeriesExprType),
      Parameter("stat", "statistic name", DataType.StringType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val t = params(0).asInstanceOf[TimeSeriesExpr]
      val s = params(1).asInstanceOf[String]
      context.copy(stack = FilterExpr.Stat(t, s) :: context.stack)
    }

    override def summary: String =
      """
        |Create a summary line showing the value of the specified statistic for the input line.
        |Valid statistic values are `avg`, `max`, `min`, `last`, and `total`. For example:
        |
        || Input          |   0 |   5 |   1 |   3 |   1 | NaN |
        ||----------------|-----|-----|-----|-----|-----|-----|
        || `avg,:stat`    |   2 |   2 |   2 |   2 |   2 |   2 |
        || `max,:stat`    |   5 |   5 |   5 |   5 |   5 |   5 |
        || `min,:stat`    |   0 |   0 |   0 |   0 |   0 |   0 |
        || `last,:stat`   |   1 |   1 |   1 |   1 |   1 |   1 |
        || `total,:stat`  |  10 |  10 |  10 |  10 |  10 |  10 |
        || `count,:stat`  |   5 |   5 |   5 |   5 |   5 |   5 |
        |
        |When used with [filter](filter-filter) the corresponding `stat-$(name)` operation can be
        |used to simplify filtering based on stats.
        |
        |```
        |name,requestsPerSecond,:eq,:sum,
        |:dup,max,:stat,50,:lt,
        |:over,min,:stat,100,:gt,
        |:or,:filter
        |```
        |
        |Could be rewritten as:
        |
        |```
        |name,requestsPerSecond,:eq,:sum,
        |:stat-max,50,:lt,:stat-min,100,:gt,:or,:filter
        |```
        |
        |The `stat-min` and `stat-max` operations will get rewritten to be the corresponding
        |call to `stat` on the first expression passed to `filter`.
      """.stripMargin.trim

    override def examples: List[String] =
      List(
        "name,sps,:eq,:sum,avg",
        "name,sps,:eq,:sum,max",
        "name,sps,:eq,:sum,min",
        "name,sps,:eq,:sum,last",
        "name,sps,:eq,:sum,total",
        "name,sps,:eq,:sum,count"
      )
  }

  trait StatWord extends TypedWord {

    override def parameters: IndexedSeq[Parameter] = ArraySeq.empty

    override def outputs: IndexedSeq[DataType] = ArraySeq(TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      context.copy(stack = value :: context.stack)
    }

    override def examples: List[String] = List("", "name,sps,:eq,:sum")

    def value: FilterExpr
  }

  case object StatAvg extends StatWord {

    override def name: String = "stat-avg"

    def value: FilterExpr = FilterExpr.StatAvg

    override def summary: String =
      """
        |Represents the `avg,:stat` line when used with the filter operation.
      """.stripMargin.trim
  }

  case object StatMax extends StatWord {

    override def name: String = "stat-max"

    def value: FilterExpr = FilterExpr.StatMax

    override def summary: String =
      """
        |Represents the `max,:stat` line when used with the filter operation.
      """.stripMargin.trim
  }

  case object StatMin extends StatWord {

    override def name: String = "stat-min"

    def value: FilterExpr = FilterExpr.StatMin

    override def summary: String =
      """
        |Represents the `min,:stat` line when used with the filter operation.
      """.stripMargin.trim
  }

  case object StatLast extends StatWord {

    override def name: String = "stat-last"

    def value: FilterExpr = FilterExpr.StatLast

    override def summary: String =
      """
        |Represents the `last,:stat` line when used with the filter operation.
      """.stripMargin.trim
  }

  case object StatCount extends StatWord {

    override def name: String = "stat-count"

    def value: FilterExpr = FilterExpr.StatCount

    override def summary: String =
      """
        |Represents the `count,:stat` line when used with the filter operation.
      """.stripMargin.trim
  }

  case object StatTotal extends StatWord {

    override def name: String = "stat-total"

    def value: FilterExpr = FilterExpr.StatTotal

    override def summary: String =
      """
        |Represents the `total,:stat` line when used with the filter operation.
      """.stripMargin.trim
  }

  case object Filter extends TypedWord {

    override def name: String = "filter"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("t1", "input time series", TimeSeriesExprType),
      Parameter("t2", "filter expression", TimeSeriesExprType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val t1 = params(0).asInstanceOf[TimeSeriesExpr]
      val t2 = params(1).asInstanceOf[TimeSeriesExpr]
      context.copy(stack = FilterExpr.Filter(t1, rewriteStatExprs(t1, t2)) :: context.stack)
    }

    private def rewriteStatExprs(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr = {
      val r = t2.rewrite {
        case s: FilterExpr.StatExpr => FilterExpr.Stat(t1, s.name, Some(s.toString))
      }
      r.asInstanceOf[TimeSeriesExpr]
    }

    override def summary: String =
      """
        |Filter the output based on another expression. For example, only show lines that have
        |a value greater than 50.
      """.stripMargin.trim

    override def examples: List[String] =
      List("name,sps,:eq,:sum,(,nf.cluster,),:by,:stat-max,30e3,:gt")
  }

  case class PriorityK(name: String, op: (TimeSeriesExpr, String, Int) => FilterExpr)
      extends TypedWord
      with StylePassthrough {

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("", "input time series", TimeSeriesExprType),
      Parameter("stat", "statistic name", DataType.StringType),
      Parameter("k", "number of results", DataType.IntType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val t = params(0).asInstanceOf[TimeSeriesExpr]
      val s = params(1).asInstanceOf[String]
      val k = params(2).asInstanceOf[Int]
      context.copy(stack = op(t, s, k) :: context.stack)
    }

    override def summary: String =
      """
        |Limit the output to the `K` time series with the highest priority values for the
        |specified summary statistic.
      """.stripMargin.trim

    override def examples: List[String] =
      List("name,sps,:eq,:sum,(,nf.cluster,),:by,max,5")
  }

  case object Consolidate extends TypedWord with StylePassthrough {

    override def name: String = "consolidate"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("", "input time series", TimeSeriesExprType),
      Parameter("cf", "consolidation function", ConsolidationFunctionType),
      Parameter("newStep", "new step size", DataType.DurationType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val t = params(0).asInstanceOf[TimeSeriesExpr]
      val cf = params(1).asInstanceOf[ConsolidationFunction]
      val step = params(2).asInstanceOf[Duration]
      context.copy(stack = FilterExpr.Consolidate(t, cf, step) :: context.stack)
    }

    override def summary: String =
      """
        |Computes a consolidated time series if the new step is larger than the step size for the
        |graph. If new step is not an even multiple, the consolidated series will be rounded up
        |to the next even multiple.
      """.stripMargin.trim

    override def examples: List[String] = List(":random,avg,5m")
  }
}
