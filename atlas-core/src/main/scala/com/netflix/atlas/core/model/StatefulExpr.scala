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
package com.netflix.atlas.core.model

import java.time.Duration
import com.netflix.atlas.core.algorithm.OnlineAlgorithm
import com.netflix.atlas.core.algorithm.OnlineDelay
import com.netflix.atlas.core.algorithm.OnlineDes
import com.netflix.atlas.core.algorithm.OnlineIgnoreN
import com.netflix.atlas.core.algorithm.OnlineRollingMax
import com.netflix.atlas.core.algorithm.OnlineRollingMin
import com.netflix.atlas.core.algorithm.OnlineSlidingDes
import com.netflix.atlas.core.algorithm.Pipeline
import com.netflix.atlas.core.algorithm.AlgoState
import com.netflix.atlas.core.algorithm.OnlineDerivative
import com.netflix.atlas.core.algorithm.OnlineIntegral
import com.netflix.atlas.core.algorithm.OnlineRollingCount
import com.netflix.atlas.core.algorithm.OnlineRollingMean
import com.netflix.atlas.core.algorithm.OnlineRollingSum
import com.netflix.atlas.core.algorithm.OnlineTrend
import com.netflix.atlas.core.stacklang.Interpreter

trait StatefulExpr extends TimeSeriesExpr {}

object StatefulExpr {

  /**
    * Compute a moving average for values within the input time series. The duration will
    * be rounded down to the nearest step boundary.
    */
  case class Trend(expr: TimeSeriesExpr, window: Duration) extends OnlineExpr {

    override protected def name: String = "trend"

    override protected def newAlgorithmInstance(context: EvalContext): OnlineAlgorithm = {
      // IgnoreN of 0 is used as an identity algorithm if the specified window is small
      // enough that no averaging will take place
      val period = (window.toMillis / context.step).toInt
      if (period <= 1) OnlineIgnoreN(0) else OnlineTrend(period)
    }

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, window, Interpreter.WordToken(":trend"))
    }
  }

  /**
    * Sum the values across the evaluation context. This is typically used to approximate the
    * distinct number of events that occurred. If the input is non-negative, then each datapoint
    * for the output line will represent the area under the input line from the start of the graph
    * to the time for that datapoint. Missing values, `NaN`, will be treated as zeroes.
    */
  case class Integral(expr: TimeSeriesExpr) extends OnlineExpr {

    override protected def name: String = "integral"

    override protected def newAlgorithmInstance(context: EvalContext): OnlineAlgorithm = {
      OnlineIntegral(Double.NaN)
    }

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, Interpreter.WordToken(s":$name"))
    }
  }

  /**
    * Determine the rate of change per step interval for the input time series.
    */
  case class Derivative(expr: TimeSeriesExpr) extends OnlineExpr {

    override protected def name: String = "derivative"

    override protected def newAlgorithmInstance(context: EvalContext): OnlineAlgorithm = {
      OnlineDerivative(Double.NaN)
    }

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, Interpreter.WordToken(s":$name"))
    }
  }

  /**
    * Delay the input time series by `n` intervals. This can be useful for alerting to see
    * if recent trends deviate from delayed trends.
    */
  case class Delay(expr: TimeSeriesExpr, n: Int) extends OnlineExpr {

    override protected def name: String = "delay"

    override protected def newAlgorithmInstance(context: EvalContext): OnlineAlgorithm = {
      OnlineDelay(n)
    }

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, n, Interpreter.WordToken(s":$name"))
    }
  }

  /**
    * Computes the number of true values over the last `n` intervals.
    */
  case class RollingCount(expr: TimeSeriesExpr, n: Int) extends OnlineExpr {

    override protected def name: String = "rolling-count"

    override protected def newAlgorithmInstance(context: EvalContext): OnlineAlgorithm = {
      OnlineRollingCount(n)
    }

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, n, Interpreter.WordToken(s":$name"))
    }
  }

  /**
    * Computes the minimum value over the last `n` intervals.
    */
  case class RollingMin(expr: TimeSeriesExpr, n: Int) extends OnlineExpr {

    override protected def name: String = "rolling-min"

    override protected def newAlgorithmInstance(context: EvalContext): OnlineAlgorithm = {
      OnlineRollingMin(n)
    }

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, n, Interpreter.WordToken(s":$name"))
    }
  }

  /**
    * Computes the maximum value over the last `n` intervals.
    */
  case class RollingMax(expr: TimeSeriesExpr, n: Int) extends OnlineExpr {

    override protected def name: String = "rolling-max"

    override protected def newAlgorithmInstance(context: EvalContext): OnlineAlgorithm = {
      OnlineRollingMax(n)
    }

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, n, Interpreter.WordToken(s":$name"))
    }
  }

  /**
    * Computes the mean of the values over the last `n` intervals.
    */
  case class RollingMean(expr: TimeSeriesExpr, n: Int, minNumValues: Int) extends OnlineExpr {

    override protected def name: String = "rolling-mean"

    override protected def newAlgorithmInstance(context: EvalContext): OnlineAlgorithm = {
      OnlineRollingMean(n, minNumValues)
    }

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, n, minNumValues, Interpreter.WordToken(s":$name"))
    }
  }

  /**
    * Computes the sum of the values over the last `n` intervals.
    */
  case class RollingSum(expr: TimeSeriesExpr, n: Int) extends OnlineExpr {

    override protected def name: String = "rolling-sum"

    override protected def newAlgorithmInstance(context: EvalContext): OnlineAlgorithm = {
      OnlineRollingSum(n)
    }

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, n, Interpreter.WordToken(s":$name"))
    }
  }

  /**
    * DES expression. In order to get the same results, it must be replayed from the same
    * starting point. Used sliding DES if deterministic results are important.
    */
  case class Des(expr: TimeSeriesExpr, trainingSize: Int, alpha: Double, beta: Double)
      extends OnlineExpr {

    override protected def name: String = "des"

    override protected def newAlgorithmInstance(context: EvalContext): OnlineAlgorithm = {
      OnlineDes(trainingSize, alpha, beta)
    }

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, trainingSize, alpha, beta, Interpreter.WordToken(s":$name"))
    }
  }

  /**
    * Sliding DES expression. In order to keep the values deterministic the start time must
    * be aligned to a step boundary. As a result, the initial gap before predicted values
    * start showing up will be the offset to align to a step boundary plus the training
    * window.
    */
  case class SlidingDes(expr: TimeSeriesExpr, trainingSize: Int, alpha: Double, beta: Double)
      extends OnlineExpr {

    override protected def name: String = "sdes"

    override protected def newAlgorithmInstance(context: EvalContext): OnlineAlgorithm = {
      val sdes = OnlineSlidingDes(trainingSize, alpha, beta)
      val alignedStart = getAlignedStartTime(context)
      val alignedOffset = ((alignedStart - context.start) / context.step).toInt
      if (alignedOffset > 0)
        Pipeline(OnlineIgnoreN(alignedOffset), sdes)
      else
        sdes
    }

    private def getAlignedStartTime(context: EvalContext): Long = {
      val trainingStep = context.step * trainingSize
      if (context.start % trainingStep == 0)
        context.start
      else
        context.start / trainingStep * trainingStep + trainingStep
    }

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, trainingSize, alpha, beta, Interpreter.WordToken(s":$name"))
    }
  }

  /**
    * Base type for stateful expressions that are based on an implementation of
    * OnlineAlgorithm.
    */
  trait OnlineExpr extends StatefulExpr {

    type StateMap = scala.collection.mutable.HashMap[ItemId, AlgoState]

    protected def name: String

    protected def expr: TimeSeriesExpr

    protected def newAlgorithmInstance(context: EvalContext): OnlineAlgorithm

    def dataExprs: List[DataExpr] = expr.dataExprs

    def isGrouped: Boolean = expr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

    def finalGrouping: List[String] = expr.finalGrouping

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      val state = rs.state.getOrElse(this, new StateMap).asInstanceOf[StateMap]

      // Update expressions with data
      val newData = rs.data.map { t =>
        val bounded = t.data.bounded(context.start, context.end)
        val length = bounded.data.length
        val algo = state.get(t.id).fold(newAlgorithmInstance(context)) { s =>
          OnlineAlgorithm(s)
        }
        var i = 0
        while (i < length) {
          bounded.data(i) = algo.next(bounded.data(i))
          i += 1
        }
        if (algo.isEmpty)
          state -= t.id
        else
          state(t.id) = algo.state
        TimeSeries(t.tags, s"$name(${t.label})", bounded)
      }

      // Update the stateful buffers for expressions that do not have an explicit value for
      // this interval. For streaming contexts only data that is reported for that interval
      // will be present, but the state needs to be moved for all entries.
      val noDataIds = state.keySet.diff(rs.data.map(_.id).toSet)
      noDataIds.foreach { id =>
        val algo = OnlineAlgorithm(state(id))
        algo.next(Double.NaN)
        if (algo.isEmpty)
          state -= id
        else
          state(id) = algo.state
      }

      ResultSet(this, newData, rs.state + (this -> state))
    }
  }
}
