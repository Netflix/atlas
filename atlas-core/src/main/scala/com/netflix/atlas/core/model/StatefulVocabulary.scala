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
import com.netflix.atlas.core.stacklang.StandardVocabulary.Macro
import com.netflix.atlas.core.stacklang.TypedMacro
import com.netflix.atlas.core.stacklang.TypedWord
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word
import com.netflix.atlas.core.stacklang.ast.DataType
import com.netflix.atlas.core.stacklang.ast.Parameter

object StatefulVocabulary extends Vocabulary {

  import com.netflix.atlas.core.model.ModelDataTypes.*

  val name: String = "stateful"

  val dependsOn: List[Vocabulary] = List(MathVocabulary)

  val words: List[Word] = List(
    Delay,
    RollingCount,
    RollingMin,
    RollingMax,
    RollingMean,
    RollingSum,
    Des,
    SlidingDes,
    Trend,
    Integral,
    Derivative,
    desTypedMacro("des-simple", List("10", "0.1", "0.5", ":des")),
    desTypedMacro("des-fast", List("10", "0.1", "0.02", ":des")),
    desTypedMacro("des-slower", List("10", "0.05", "0.03", ":des")),
    desTypedMacro("des-slow", List("10", "0.03", "0.04", ":des")),
    desTypedMacro("sdes-simple", List("10", "0.1", "0.5", ":sdes")),
    desTypedMacro("sdes-fast", List("10", "0.1", "0.02", ":sdes")),
    desTypedMacro("sdes-slower", List("10", "0.05", "0.03", ":sdes")),
    desTypedMacro("sdes-slow", List("10", "0.03", "0.04", ":sdes")),
    Macro("des-epic-signal", desEpicSignal, List("name,sps,:eq,:sum,10,0.1,0.5,0.2,0.2,4"))
  )

  private def desTypedMacro(name: String, body: List[String]): TypedMacro = {
    val fullBody = (":dup" :: body) ::: List(name, ":named-rewrite")
    val desType = if (name.startsWith("sdes")) "sliding DES" else "DES"
    TypedMacro(
      name,
      fullBody,
      ArraySeq(Parameter("", "input time series", TimeSeriesExprType)),
      ArraySeq(TimeSeriesExprType),
      s"Apply $desType with preset parameters.",
      List("42")
    )
  }

  case object Delay extends TypedWord with StylePassthrough {

    override def name: String = "delay"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("", "input time series", TimeSeriesExprType),
      Parameter("n", "number of datapoints to delay", DataType.IntType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val t = params(0).asInstanceOf[TimeSeriesExpr]
      val v = params(1).asInstanceOf[Int]
      context.copy(stack = StatefulExpr.Delay(t, v) :: context.stack)
    }

    override def summary: String =
      """
        |Delays the values by the window size. This is similar to the `:offset` operator
        |except that it can be applied to any input line instead of just changing the time
        |window fetched with a DataExpr. Short delays can be useful for alerting to detect
        |changes in slightly shifted trend lines.
        |
        |The window size, `n`, is the number of datapoints to consider, including the current
        |value. Note that it is based on datapoints, not a specific amount of time. As a result,
        |the number of occurrences will be reduced when transitioning to a larger time frame
        |that causes consolidation.
        |
        | Since: 1.6
      """.stripMargin.trim

    override def examples: List[String] = List("name,requestsPerSecond,:eq,:sum,5")
  }

  case object RollingCount extends TypedWord with StylePassthrough {

    override def name: String = "rolling-count"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("", "input time series", TimeSeriesExprType),
      Parameter("n", "window size", DataType.IntType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val t = params(0).asInstanceOf[TimeSeriesExpr]
      val v = params(1).asInstanceOf[Int]
      context.copy(stack = StatefulExpr.RollingCount(t, v) :: context.stack)
    }

    override def summary: String =
      """
        |Number of occurrences within a specified window. This operation is frequently used in
        |alerting expressions to reduce noise. For example:
        |
        |```
        |# Check to see if average cpu usage is > 80%
        |name,cpuUser,:eq,:avg,80,:gt,
        |
        |# Only alert if that is true for more than 3 of the last 5
        |# datapoints
        |5,:rolling-count,3,:gt
        |```
        |
        |A value is counted if it is non-zero. Missing values, `NaN`, will be treated as zeroes.
        |For example:
        |
        || Input | 3,:rolling-count |
        ||-------|------------------|
        || 0     | 0                |
        || 1     | 1                |
        || -1    | 2                |
        || NaN   | 2                |
        || 0     | 1                |
        || 1     | 1                |
        || 1     | 2                |
        || 1     | 3                |
        || 1     | 3                |
        || 0     | 2                |
        |
        |The window size, `n`, is the number of datapoints to consider, including the current
        |value. Note that it is based on datapoints, not a specific amount of time. As a result,
        |the number of occurrences will be reduced when transitioning to a larger time frame
        |that causes consolidation.
        |
        |
      """.stripMargin.trim

    override def examples: List[String] = List(":random,0.4,:gt,5")
  }

  case object RollingMin extends TypedWord with StylePassthrough {

    override def name: String = "rolling-min"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("", "input time series", TimeSeriesExprType),
      Parameter("n", "window size", DataType.IntType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val t = params(0).asInstanceOf[TimeSeriesExpr]
      val v = params(1).asInstanceOf[Int]
      context.copy(stack = StatefulExpr.RollingMin(t, v) :: context.stack)
    }

    override def summary: String =
      """
        |Minimum value within a specified window. This operation can be used in
        |alerting expressions to find a lower bound for noisy data based on recent
        |samples. For example:
        |
        |```
        |name,sps,:eq,:sum,
        |:dup,
        |5,:rolling-min
        |```
        |
        |Missing values, `NaN`, will be ignored when computing the min. If all values
        |within the window are `NaN`, then `NaN` will be emitted. For example:
        |
        || Input | 3,:rolling-min   |
        ||-------|------------------|
        || 0     | 0                |
        || 1     | 0                |
        || -1    | -1               |
        || NaN   | -1               |
        || 0     | -1               |
        || 1     | 0                |
        || 1     | 0                |
        || 1     | 1                |
        || 1     | 1                |
        || 0     | 0                |
        |
        |The window size, `n`, is the number of datapoints to consider including the current
        |value. Note that it is based on datapoints not a specific amount of time. As a result the
        |number of occurrences will be reduced when transitioning to a larger time frame that
        |causes consolidation.
        |
        |Since: 1.6
      """.stripMargin.trim

    override def examples: List[String] = List("name,sps,:eq,:sum,5")
  }

  case object RollingMax extends TypedWord with StylePassthrough {

    override def name: String = "rolling-max"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("", "input time series", TimeSeriesExprType),
      Parameter("n", "window size", DataType.IntType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val t = params(0).asInstanceOf[TimeSeriesExpr]
      val v = params(1).asInstanceOf[Int]
      context.copy(stack = StatefulExpr.RollingMax(t, v) :: context.stack)
    }

    override def summary: String =
      """
        |Maximum value within a specified window. This operation can be used in
        |alerting expressions to find a lower bound for noisy data based on recent
        |samples. For example:
        |
        |```
        |name,sps,:eq,:sum,
        |:dup,
        |5,:rolling-max
        |```
        |
        |Missing values, `NaN`, will be ignored when computing the min. If all values
        |within the window are `NaN`, then `NaN` will be emitted. For example:
        |
        || Input | 3,:rolling-max   |
        ||-------|------------------|
        || 0     | 0                |
        || 1     | 1                |
        || -1    | 1                |
        || NaN   | 1                |
        || 0     | 0                |
        || 1     | 1                |
        || 1     | 1                |
        || 1     | 1                |
        || 1     | 1                |
        || 0     | 1                |
        |
        |The window size, `n`, is the number of datapoints to consider including the current
        |value. Note that it is based on datapoints not a specific amount of time. As a result the
        |number of occurrences will be reduced when transitioning to a larger time frame that
        |causes consolidation.
        |
        |Since: 1.6
      """.stripMargin.trim

    override def examples: List[String] = List("name,sps,:eq,:sum,5")
  }

  case object RollingMean extends TypedWord with StylePassthrough {

    override def name: String = "rolling-mean"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("", "input time series", TimeSeriesExprType),
      Parameter("n", "window size", DataType.IntType),
      Parameter("minNumValues", "minimum non-NaN values required", DataType.IntType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val t = params(0).asInstanceOf[TimeSeriesExpr]
      val n = params(1).asInstanceOf[Int]
      val m = params(2).asInstanceOf[Int]
      context.copy(stack = StatefulExpr.RollingMean(t, n, m) :: context.stack)
    }

    override def summary: String =
      """
        |Mean of the values within a specified window. The mean will only be emitted
        |if there are at least a minimum number of actual values (not `NaN`) within
        |the window. Otherwise `NaN` will be emitted for that time period.
        |
        || Input | 3,2,:rolling-mean   |
        ||-------|---------------------|
        || 0     | NaN                 |
        || 1     | 0.5                 |
        || -1    | 0.0                 |
        || NaN   | 0.0                 |
        || NaN   | NaN                 |
        || 0     | NaN                 |
        || 1     | 0.5                 |
        || 1     | 0.667               |
        || 1     | 1                   |
        || 0     | 0.667               |
        |
        |The window size, `n`, is the number of datapoints to consider including the current
        |value. There must be at least `minNumValues` non-NaN values within that window before
        |it will emit a mean. Note that it is based on datapoints, not a specific amount of time.
        |As a result the number of occurrences will be reduced when transitioning to a larger time
        |frame that causes consolidation.
        |
        |Since: 1.6
      """.stripMargin.trim

    override def examples: List[String] = List("name,sps,:eq,:sum,5,3")
  }

  case object RollingSum extends TypedWord with StylePassthrough {

    override def name: String = "rolling-sum"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("", "input time series", TimeSeriesExprType),
      Parameter("n", "window size", DataType.IntType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val t = params(0).asInstanceOf[TimeSeriesExpr]
      val n = params(1).asInstanceOf[Int]
      context.copy(stack = StatefulExpr.RollingSum(t, n) :: context.stack)
    }

    override def summary: String =
      """
        |Sum of the values within a specified window.
        |
        || Input | 3,:rolling-sum    |
        ||-------|---------------------|
        || 0     | 0.0                 |
        || 1     | 1.0                 |
        || -1    | 0.0                 |
        || NaN   | 0.0                 |
        || NaN   | -1.0                |
        || NaN   | NaN                 |
        || 1     | 1.0                 |
        || 1     | 2.0                 |
        || 1     | 3.0                 |
        || 0     | 2.0                 |
        |
        |The window size, `n`, is the number of datapoints to consider including the current
        |value. Note that it is based on datapoints, not a specific amount of time.
        |As a result the number of occurrences will be reduced when transitioning to a larger time
        |frame that causes consolidation.
        |
        |Since: 1.6
      """.stripMargin.trim

    override def examples: List[String] = List("name,sps,:eq,:sum,5,3")
  }

  case object Des extends TypedWord with StylePassthrough {

    override def name: String = "des"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("", "input time series", TimeSeriesExprType),
      Parameter("training", "training window size", DataType.IntType),
      Parameter("alpha", "data smoothing factor", DataType.DoubleType),
      Parameter("beta", "trend smoothing factor", DataType.DoubleType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val t = params(0).asInstanceOf[TimeSeriesExpr]
      val n = params(1).asInstanceOf[Int]
      val a = params(2).asInstanceOf[Double]
      val b = params(3).asInstanceOf[Double]
      context.copy(stack = StatefulExpr.Des(t, n, a, b) :: context.stack)
    }

    override def summary: String =
      """
        |[Double exponential smoothing](DES). For most use-cases [sliding DES](stateful-sdes)
        |should be used instead to ensure a deterministic prediction.
      """.stripMargin.trim

    override def examples: List[String] = List("name,requestsPerSecond,:eq,:sum,5,0.1,0.5")
  }

  case object SlidingDes extends TypedWord with StylePassthrough {

    override def name: String = "sdes"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("", "input time series", TimeSeriesExprType),
      Parameter("training", "training window size", DataType.IntType),
      Parameter("alpha", "data smoothing factor", DataType.DoubleType),
      Parameter("beta", "trend smoothing factor", DataType.DoubleType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val t = params(0).asInstanceOf[TimeSeriesExpr]
      val n = params(1).asInstanceOf[Int]
      val a = params(2).asInstanceOf[Double]
      val b = params(3).asInstanceOf[Double]
      context.copy(stack = StatefulExpr.SlidingDes(t, n, a, b) :: context.stack)
    }

    override def summary: String =
      """
        |Variant of [:des](stateful-des) that is deterministic as long as the step size does not
        |change. One of the common complaints with DES is that to get the same value for a given
        |time you must start feeding in data at exactly the same time. So for normal graphs
        |where it is computed using the window of the chart it will have slightly different
        |predictions for a given time. As it is often used for alerting this makes it
        |cumbersome to try and determine:
        |
        |1. Why an alarm fired
        |2. When alarms would have fired for tuning
        |
        |Sliding DES uses two DES functions and alternates between them. One will get trained
        |while the other is getting used, and then the one that was getting used will get reset and
        |the roles swapped.
        |
        |```
        | F1 | A |-- T1 --|-- P1 --|-- T1 --|-- P1 --|-- T1 --|
        | F2 | A |        |-- T2 --|-- P2 --|-- T2 --|-- P2 --|
        |
        |Result:
        |
        | R  |-- NaN -----|-- P1 --|-- P2 --|-- P1 --|-- P2 --|
        |```
        |
        |Both functions will ignore any data until it reaches a boundary, even multiple, of the
        |training window. That is shown as `A` in the diagram above. The first function will
        |then start training, `T1`, and after the training window the first predicted values, `P1`,
        |will get generated. The ouput line will alternate between the predictions from both
        |DES functions.
        |
        |The alternation between functions can cause the prediction line to look choppier than
        |DES, e.g., on a gradual drop:
        |
        |![Gradual Drop](images/sdes-gradual-example.png)
        |
        |Further, since each prediction only considers data for a narrow window it will adjust to
        |sharp changes faster. For example:
        |
        |![Sharp Drop](images/sdes-sharp-example.png)
        |
        |Since: 1.5.0
      """.stripMargin.trim

    override def examples: List[String] = List("name,requestsPerSecond,:eq,:sum,5,0.1,0.5")
  }

  case object Trend extends TypedWord with StylePassthrough {

    override def name: String = "trend"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("", "input time series", TimeSeriesExprType),
      Parameter("window", "moving average window", DataType.DurationType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val t = params(0).asInstanceOf[TimeSeriesExpr]
      val d = params(1).asInstanceOf[Duration]
      context.copy(stack = StatefulExpr.Trend(t, d) :: context.stack)
    }

    override def summary: String =
      """
        |Computes a moving average over the input window. Until there is at least one sample
        |for the whole window it will emit `NaN`. If the input line has `NaN` values, then they
        |will be treated as zeros. Example:
        |
        || Input | 2m,:trend | 5m,:trend |
        ||-------|-----------|-----------|
        ||   0   |  NaN      | NaN       |
        ||   1   |  0.5      | NaN       |
        ||  -1   |  0.0      | NaN       |
        || NaN   | -0.5      | NaN       |
        ||   0   |  0.0      | 0.0       |
        ||   1   |  0.5      | 0.2       |
        ||   2   |  1.5      | 0.4       |
        ||   1   |  1.5      | 0.8       |
        ||   1   |  1.0      | 1.0       |
        ||   0   |  0.5      | 1.0       |
        |
        |The window size is specified as a range of time. If the window size is not evenly
        |divisible by the [step size](Concepts#step-size), then the window size will be rounded
        |down. So a 5m window with a 2m step would result in a 4m window with two datapoints
        |per average. A step size larger than the window will result in the trend being a no-op.
      """.stripMargin.trim

    override def deprecated: Option[String] =
      Some("Use :rolling-mean instead")

    override def examples: List[String] = List(":random,PT5M", ":random,20m")
  }

  case object Integral extends TypedWord with StylePassthrough {

    override def name: String = "integral"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("", "input time series", TimeSeriesExprType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val t = params(0).asInstanceOf[TimeSeriesExpr]
      context.copy(stack = StatefulExpr.Integral(t) :: context.stack)
    }

    override def summary: String =
      """
        |Sum the values across the evaluation context. This is typically used to approximate the
        |distinct number of events that occurred. If the input is non-negative, then each datapoint
        |for the output line will represent the area under the input line from the start of the
        |graph to the time for that datapoint. Missing values, `NaN`, will be treated as zeroes.
        |For example:
        |
        || Input | :integral |
        ||-------|-----------|
        || 0     | 0         |
        || 1     | 1         |
        || -1    | 0         |
        || NaN   | 0         |
        || 0     | 0         |
        || 1     | 1         |
        || 2     | 3         |
        || 1     | 4         |
        || 1     | 5         |
        || 0     | 5         |
        |
        |For a [counter](http://netflix.github.io/spectator/en/latest/intro/counter/), each data
        |point represents the average rate per second over the step interval. To compute the total
        |amount incremented, the value first needs to be converted to a rate per step interval.
        |This conversion can be performed using the [:per-step](math-per‐step) operation.
      """.stripMargin.trim

    override def examples: List[String] = List("1", "name,requestsPerSecond,:eq,:sum,:per-step")
  }

  case object Derivative extends TypedWord with StylePassthrough {

    override def name: String = "derivative"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("", "input time series", TimeSeriesExprType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val t = params(0).asInstanceOf[TimeSeriesExpr]
      context.copy(stack = StatefulExpr.Derivative(t) :: context.stack)
    }

    override def summary: String =
      """
        |Opposite of [:integral](stateful-integral). Computes the rate of change per step of the
        |input time series.
      """.stripMargin.trim

    override def examples: List[String] = List("1", "1,:integral")
  }

  private def desEpicSignal = List(
    // Parameters
    "noise",
    ":sset",
    "minPercent",
    ":sset",
    "maxPercent",
    ":sset",
    "beta",
    ":sset",
    "alpha",
    ":sset",
    "trainingSize",
    ":sset",
    "line",
    ":sset",
    // Compute a DES prediction
    "pred",
    "line",
    ":get",
    "trainingSize",
    ":get",
    "alpha",
    ":get",
    "beta",
    ":get",
    ":des",
    ":set",
    // Line for minimum bound using noise param
    "minPredNoiseBound",
    "pred",
    ":get",
    "noise",
    ":get",
    ":sub",
    ":set",
    // Line for minimum bound using minPercent param
    "minPredPercentBound",
    "pred",
    ":get",
    "1.0",
    ":const",
    "minPercent",
    ":get",
    ":const",
    ":fsub",
    ":fmul",
    ":set",
    // Line for maximum bound using noise param
    "maxPredNoiseBound",
    "pred",
    ":get",
    "noise",
    ":get",
    ":add",
    ":set",
    // Line for maximum bound using maxPercent param
    "maxPredPercentBound",
    "pred",
    ":get",
    "2.0",
    ":const",
    "1.0",
    ":const",
    "maxPercent",
    ":get",
    ":const",
    ":fsub",
    ":fsub",
    ":fmul",
    ":set",
    // Signal indicating if it is below both lower bounds
    "line",
    ":get",
    "minPredNoiseBound",
    ":get",
    ":lt",
    "line",
    ":get",
    "minPredPercentBound",
    ":get",
    ":lt",
    ":and",
    // Signal indicating if it is above both upper bounds
    "line",
    ":get",
    "maxPredNoiseBound",
    ":get",
    ":gt",
    "line",
    ":get",
    "maxPredPercentBound",
    ":get",
    ":gt",
    ":and",
    // True if it deviates from the upper or lower bound
    ":or"
  )

}
