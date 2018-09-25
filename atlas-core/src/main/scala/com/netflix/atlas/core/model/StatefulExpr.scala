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
import com.netflix.atlas.core.util.Math
import com.typesafe.config.Config

trait StatefulExpr extends TimeSeriesExpr {}

object StatefulExpr {

  //
  // RollingCount
  //

  case class RollingCount(expr: TimeSeriesExpr, n: Int) extends StatefulExpr {
    import com.netflix.atlas.core.model.StatefulExpr.RollingCount._

    def dataExprs: List[DataExpr] = expr.dataExprs

    override def toString: String = s"$expr,$n,:rolling-count"

    def isGrouped: Boolean = expr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

    def finalGrouping: List[String] = expr.finalGrouping

    private def eval(ts: ArrayTimeSeq, s: State): State = {
      val data = ts.data
      var pos = s.pos
      var value = s.value
      val buf = s.buf.clone()
      var i = 0
      while (i < data.length) {
        if (pos < n) {
          buf(pos % n) = data(i)
          value = value + Math.toBooleanDouble(data(i))
          data(i) = value
        } else {
          val p = pos % n
          value = value - Math.toBooleanDouble(buf(p))
          value = value + Math.toBooleanDouble(data(i))
          buf(p) = data(i)
          data(i) = value
        }
        pos += 1
        i += 1
      }
      State(pos, value, buf)
    }

    private def newState: State = {
      State(0, 0.0, new Array[Double](n))
    }

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      if (n <= 1) rs
      else {
        val state = rs.state.getOrElse(this, new StateMap).asInstanceOf[StateMap]
        val newData = rs.data.map { t =>
          val bounded = t.data.bounded(context.start, context.end)
          val s = state.getOrElse(t.id, newState)
          state(t.id) = eval(bounded, s)
          TimeSeries(t.tags, s"rolling-count(${t.label}, $n)", bounded)
        }
        ResultSet(this, newData, rs.state + (this -> state))
      }
    }
  }

  object RollingCount {

    case class State(pos: Int, value: Double, buf: Array[Double])

    type StateMap = scala.collection.mutable.AnyRefMap[ItemId, State]
  }

  //
  // Trend
  //

  case class Trend(expr: TimeSeriesExpr, window: Duration) extends StatefulExpr {
    import com.netflix.atlas.core.model.StatefulExpr.Trend._

    def dataExprs: List[DataExpr] = expr.dataExprs

    override def toString: String = s"$expr,$window,:trend"

    def isGrouped: Boolean = expr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

    def finalGrouping: List[String] = expr.finalGrouping

    private def eval(period: Int, ts: ArrayTimeSeq, s: State): State = {
      val data = ts.data
      var nanCount = s.nanCount
      var pos = s.pos
      var value = s.value
      val buf = s.buf.clone()
      var i = 0
      while (i < data.length) {
        if (data(i).isNaN) nanCount += 1
        if (pos < period - 1) {
          buf(pos % period) = data(i)
          value = Math.addNaN(value, data(i))
          data(i) = Double.NaN
        } else {
          val p = pos % period
          if (buf(p).isNaN) nanCount -= 1
          value = Math.addNaN(Math.subtractNaN(value, buf(p)), data(i))
          buf(p) = data(i)
          data(i) = if (nanCount < period) value / period else Double.NaN
        }
        pos += 1
        i += 1
      }
      State(nanCount, pos, value, buf)
    }

    private def newState(period: Int): State = State(0, 0, 0.0, new Array[Double](period))

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val period = (window.toMillis / context.step).toInt
      val rs = expr.eval(context, data)
      if (period <= 1) rs
      else {
        val state = rs.state.getOrElse(this, new StateMap).asInstanceOf[StateMap]
        val newData = rs.data.map { t =>
          val bounded = t.data.bounded(context.start, context.end)
          val s = state.getOrElse(t.id, newState(period))
          state(t.id) = eval(period, bounded, s)
          TimeSeries(t.tags, s"trend(${t.label}, $window)", bounded)
        }
        ResultSet(this, newData, rs.state + (this -> state))
      }
    }
  }

  object Trend {

    case class State(nanCount: Int, pos: Int, value: Double, buf: Array[Double])

    type StateMap = scala.collection.mutable.AnyRefMap[ItemId, State]
  }

  //
  // Integral
  //

  case class Integral(expr: TimeSeriesExpr) extends StatefulExpr {
    import com.netflix.atlas.core.model.StatefulExpr.Integral._

    def dataExprs: List[DataExpr] = expr.dataExprs

    override def toString: String = s"$expr,:integral"

    def isGrouped: Boolean = expr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

    def finalGrouping: List[String] = expr.finalGrouping

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      val state = rs.state.getOrElse(this, new StateMap).asInstanceOf[StateMap]
      val newData = rs.data.map { t =>
        val bounded = t.data.bounded(context.start, context.end)
        val length = bounded.data.length
        var i = 1
        val s = state.getOrElse(t.id, newState())
        bounded.data(0) = Math.addNaN(bounded.data(0), s.value)
        while (i < length) {
          bounded.data(i) = Math.addNaN(bounded.data(i), bounded.data(i - 1))
          i += 1
        }
        state(t.id) = State(bounded.data(i - 1))
        TimeSeries(t.tags, s"integral(${t.label})", bounded)
      }
      ResultSet(this, newData, rs.state + (this -> state))
    }

    private def newState(): State = State(Double.NaN)
  }

  object Integral {

    case class State(value: Double)

    type StateMap = scala.collection.mutable.AnyRefMap[ItemId, State]
  }

  //
  // Derivative
  //
  case class Derivative(expr: TimeSeriesExpr) extends StatefulExpr {
    import com.netflix.atlas.core.model.StatefulExpr.Derivative._

    def dataExprs: List[DataExpr] = expr.dataExprs

    override def toString: String = s"$expr,:derivative"

    def isGrouped: Boolean = expr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

    def finalGrouping: List[String] = expr.finalGrouping

    private def newState(): State = State(Double.NaN)

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      val state = rs.state.getOrElse(this, new StateMap).asInstanceOf[StateMap]
      val newData = rs.data.map { t =>
        val bounded = t.data.bounded(context.start, context.end)
        val length = bounded.data.length
        var i = 1
        var prev = bounded.data(0)
        val s = state.getOrElse(t.id, newState())
        bounded.data(0) -= s.value
        while (i < length) {
          val tmp = prev
          prev = bounded.data(i)
          bounded.data(i) -= tmp
          i += 1
        }
        state(t.id) = State(prev)
        TimeSeries(t.tags, s"derivative(${t.label})", bounded)
      }
      ResultSet(this, newData, rs.state + (this -> state))
    }
  }

  object Derivative {

    case class State(value: Double)

    type StateMap = scala.collection.mutable.AnyRefMap[ItemId, State]
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

    override def toString: String = s"$expr,$n,:delay"
  }

  /**
    * Computes the minimum value over the last `n` intervals.
    */
  case class RollingMin(expr: TimeSeriesExpr, n: Int) extends OnlineExpr {

    override protected def name: String = "rolling-min"

    override protected def newAlgorithmInstance(context: EvalContext): OnlineAlgorithm = {
      OnlineRollingMin(n)
    }

    override def toString: String = s"$expr,$n,:rolling-min"
  }

  /**
    * Computes the maximum value over the last `n` intervals.
    */
  case class RollingMax(expr: TimeSeriesExpr, n: Int) extends OnlineExpr {

    override protected def name: String = "rolling-max"

    override protected def newAlgorithmInstance(context: EvalContext): OnlineAlgorithm = {
      OnlineRollingMax(n)
    }

    override def toString: String = s"$expr,$n,:rolling-max"
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

    override def toString: String = s"$expr,$trainingSize,$alpha,$beta,:des"
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

    override def toString: String = s"$expr,$trainingSize,$alpha,$beta,:sdes"
  }

  /**
    * Base type for stateful expressions that are based on an implementation of
    * OnlineAlgorithm.
    */
  trait OnlineExpr extends StatefulExpr {
    type StateMap = scala.collection.mutable.AnyRefMap[ItemId, Config]

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
        state(t.id) = algo.state
        TimeSeries(t.tags, s"$name(${t.label})", bounded)
      }
      ResultSet(this, newData, rs.state + (this -> state))
    }
  }
}
