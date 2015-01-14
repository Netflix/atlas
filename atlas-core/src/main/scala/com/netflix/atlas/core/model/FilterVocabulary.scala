/*
 * Copyright 2015 Netflix, Inc.
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

import com.netflix.atlas.core.stacklang.SimpleWord
import com.netflix.atlas.core.stacklang.StandardVocabulary.Macro
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word

object FilterVocabulary extends Vocabulary {

  import com.netflix.atlas.core.model.Extractors._

  val words: List[Word] = StatefulVocabulary.words ::: List(
    Stat, StatMax, StatMin, StatAvg, StatTotal, Filter,

    // Legacy operation equivalent to `max,:stat`
    Macro("stat-max-mf", List("max", ":stat"), List("42"))
  )

  object Stat extends SimpleWord {
    override def name: String = "stat"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: String) :: TimeSeriesType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (s: String) :: TimeSeriesType(t) :: stack => FilterExpr.Stat(t, s) :: stack
    }

    override def signature: String = "TimeSeriesExpr String -- FilterExpr"

    override def summary: String =
      """
        |Create a summary line showing the value of the specified statistic for the input line.
        |Valid statistic values are `min`, `max`, `avg`, and `total`. For example:
        |
        || Input          |  0 |  5 |  1 |  3 |  1 |
        ||----------------|----|----|----|----|----|
        || `max,:stat`    |  5 |  5 |  5 |  5 |  5 |
        || `min,:stat`    |  0 |  0 |  0 |  0 |  0 |
        || `avg,:stat`    |  2 |  2 |  2 |  2 |  2 |
        || `total,:stat`  | 10 | 10 | 10 | 10 | 10 |
        |
        |When used with [filter](#filter) the corresponding `stat-$(name)` operation can be used
        |to simplify filtering based on stats.
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

    override def examples: List[String] = List("a,b,:eq,max")
  }

  trait StatWord extends SimpleWord {

    protected def matcher: PartialFunction[List[Any], Boolean] = { case _ => true }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case stack => value :: stack
    }

    override def signature: String = " -- FilterExpr"

    override def examples: List[String] = List("", "a,b,:eq")

    def value: FilterExpr
  }

  object StatMax extends StatWord {
    override def name: String = "stat-max"

    def value: FilterExpr = FilterExpr.StatMax

    override def summary: String =
      """
        |Represents the `max,:stat` line when used with the filter operation.
      """.stripMargin.trim
  }

  object StatMin extends StatWord {
    override def name: String = "stat-min"

    def value: FilterExpr = FilterExpr.StatMin

    override def summary: String =
      """
        |Represents the `min,:stat` line when used with the filter operation.
      """.stripMargin.trim
  }

  object StatAvg extends StatWord {
    override def name: String = "stat-avg"

    def value: FilterExpr = FilterExpr.StatAvg

    override def summary: String =
      """
        |Represents the `avg,:stat` line when used with the filter operation.
      """.stripMargin.trim
  }

  object StatTotal extends StatWord {
    override def name: String = "stat-total"

    def value: FilterExpr = FilterExpr.StatTotal

    override def summary: String =
      """
        |Represents the `total,:stat` line when used with the filter operation.
      """.stripMargin.trim
  }

  object Filter extends SimpleWord {
    override def name: String = "filter"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case TimeSeriesType(_) :: TimeSeriesType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case TimeSeriesType(t2) :: TimeSeriesType(t1) :: stack =>
        FilterExpr.Filter(t1, rewriteStatExprs(t1, t2)) :: stack
    }

    private def rewriteStatExprs(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr = {
      val r = t2.rewrite {
        case s: FilterExpr.StatExpr => FilterExpr.Stat(t1, s.name)
      }
      r.asInstanceOf[TimeSeriesExpr]
    }

    override def signature: String = "TimeSeriesExpr TimeSeriesExpr -- FilterExpr"

    override def summary: String =
      """
        |Filter the output based on another expression. For example, only show lines that have
        |a value greater than 50.
      """.stripMargin.trim

    override def examples: List[String] = List("a,b,:eq,:dup,:stat-max,50,:gt")
  }
}
