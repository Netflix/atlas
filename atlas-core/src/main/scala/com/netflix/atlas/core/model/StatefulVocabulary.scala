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
import com.netflix.atlas.core.util.Strings

object StatefulVocabulary extends Vocabulary {

  import com.netflix.atlas.core.model.Extractors._

  val words: List[Word] = MathVocabulary.words ::: List(
    RollingCount, Des, Trend, Integral, Derivative,
    Macro("des-simple", List("10", "0.1",  "0.5",  ":des"), List("42")),
    Macro("des-fast",   List("10", "0.1",  "0.02", ":des"), List("42")),
    Macro("des-slower", List("10", "0.05", "0.03", ":des"), List("42")),
    Macro("des-slow",   List("10", "0.03", "0.04", ":des"), List("42")),

    Macro("des-epic-signal", desEpicSignal, List("42,10,0.1,0.5,0.2,0.2,4"))
  )

  object RollingCount extends SimpleWord {
    override def name: String = "rolling-count"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case IntType(_) :: TimeSeriesType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case IntType(v) :: TimeSeriesType(t) :: s => StatefulExpr.RollingCount(t, v) :: s
    }

    override def summary: String =
      """
        |Number of occurrences.
      """.stripMargin.trim

    override def signature: String = "TimeSeriesExpr n:Int -- TimeSeriesExpr"

    override def examples: List[String] = List("a,b,:eq,:sum,5")
  }

  object Des extends SimpleWord {
    override def name: String = "des"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: String) :: (_: String) :: (_: String) :: TimeSeriesType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case DoubleType(b) :: DoubleType(a) :: IntType(n) :: TimeSeriesType(t) :: s =>
        StatefulExpr.Des(t, n, a, b) :: s
    }

    override def summary: String =
      """
        |[Double exponential smoothing](DES).
      """.stripMargin.trim

    override def signature: String =
      "TimeSeriesExpr training:Int alpha:Double beta:Double -- TimeSeriesExpr"

    override def examples: List[String] = List("a,b,:eq,:sum,5,0.1,0.5")
  }

  object Trend extends SimpleWord {
    override def name: String = "trend"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: String) :: TimeSeriesType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (v: String) :: TimeSeriesType(t) :: s =>
        StatefulExpr.Trend(t, Strings.parseDuration(v)) :: s
    }

    override def summary: String =
      """
        |Moving average.
      """.stripMargin.trim

    override def signature: String =
      "TimeSeriesExpr window:Duration -- TimeSeriesExpr"

    override def examples: List[String] = List("a,b,:eq,:sum,PT5M", "a,b,:eq,:sum,5m")
  }

  object Integral extends SimpleWord {
    override def name: String = "integral"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case TimeSeriesType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case TimeSeriesType(t) :: s => StatefulExpr.Integral(t) :: s
    }

    override def summary: String =
      """
        |Sum the values across the evaluation context.
      """.stripMargin.trim

    override def signature: String =
      "TimeSeriesExpr -- TimeSeriesExpr"

    override def examples: List[String] = List("a,b,:eq,:sum")
  }

  object Derivative extends SimpleWord {
    override def name: String = "derivative"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case TimeSeriesType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case TimeSeriesType(t) :: s => StatefulExpr.Derivative(t) :: s
    }

    override def summary: String =
      """
        |Opposite of [:integral](#integral). Computes the rate of change per step of the input
        |time series.
      """.stripMargin.trim

    override def signature: String =
      "TimeSeriesExpr -- TimeSeriesExpr"

    override def examples: List[String] = List("a,b,:eq,:sum")
  }

  private def desEpicSignal = List(
    // Parameters
    "noise",        ":sset",
    "minPercent",   ":sset",
    "maxPercent",   ":sset",
    "beta",         ":sset",
    "alpha",        ":sset",
    "trainingSize", ":sset",
    "line",         ":sset",

    // Compute a DES prediction
    "pred",
      "line",         ":get",
      "trainingSize", ":get",
      "alpha",        ":get",
      "beta",         ":get",
      ":des",
    ":set",

    // Line for minimum bound using noise param
    "minPredNoiseBound",
      "pred",  ":get",
      "noise", ":get",
      ":sub",
    ":set",

    // Line for minimum bound using minPercent param
    "minPredPercentBound",
      "pred",       ":get",
      "1.0",        ":const",
      "minPercent", ":get", ":const",
      ":sub",
      ":mul",
    ":set",

    // Line for maximum bound using noise param
    "maxPredNoiseBound",
      "pred",  ":get",
      "noise", ":get",
      ":add",
    ":set",

    // Line for maximum bound using maxPercent param
    "maxPredPercentBound",
      "pred",       ":get",
      "2.0",        ":const",
      "1.0",        ":const",
      "maxPercent", ":get", ":const",
      ":sub",
      ":sub",
      ":mul",
    ":set",

    // Signal indicating if it is below both lower bounds
      "line",              ":get",
      "minPredNoiseBound", ":get",
      ":lt",

      "line",                ":get",
      "minPredPercentBound", ":get",
      ":lt",
    ":and",

    // Signal indicating if it is above both upper bounds
      "line",              ":get",
      "maxPredNoiseBound", ":get",
      ":gt",

      "line",                ":get",
      "maxPredPercentBound", ":get",
      ":gt",
    ":and",

    // True if it deviates from the upper or lower bound
    ":or"
  )

}
