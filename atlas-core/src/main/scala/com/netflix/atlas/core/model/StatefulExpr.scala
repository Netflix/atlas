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

import java.math.BigInteger
import java.time.Duration

import com.netflix.atlas.core.algorithm.OnlineDes
import com.netflix.atlas.core.algorithm.OnlineSlidingDes
import com.netflix.atlas.core.util.Math

trait StatefulExpr extends TimeSeriesExpr {

}

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
      if (n <= 1) rs else {
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

    type StateMap = scala.collection.mutable.AnyRefMap[BigInteger, State]
  }

  //
  // Des
  //

  case class Des(
      expr: TimeSeriesExpr,
      trainingSize: Int,
      alpha: Double,
      beta: Double) extends StatefulExpr {
    import com.netflix.atlas.core.model.StatefulExpr.Des._

    def dataExprs: List[DataExpr] = expr.dataExprs
    override def toString: String = s"$expr,$trainingSize,$alpha,$beta,:des"

    def isGrouped: Boolean = expr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

    private def eval(ts: ArrayTimeSeq, s: State): State = {
      val desF = OnlineDes(s.desState)

      val data = ts.data
      var pos = 0
      while (pos < data.length) {
        val yn = data(pos)
        data(pos) = desF.next(yn)
        pos += 1
      }
      State(desF.state)
    }

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      val state = rs.state.getOrElse(this, new StateMap).asInstanceOf[StateMap]
      val newData = rs.data.map { t =>
        val bounded = t.data.bounded(context.start, context.end)
        val s = state.getOrElse(t.id, {
          val desF = OnlineDes(trainingSize, alpha, beta)
          desF.reset()
          State(desF.state)
        })
        state(t.id) = eval(bounded, s)
        TimeSeries(t.tags, s"des(${t.label})", bounded)
      }
      ResultSet(this, newData, rs.state + (this -> state))
    }
  }

  object Des {
    case class State(desState: OnlineDes.State)

    type StateMap = scala.collection.mutable.AnyRefMap[BigInteger, State]
  }

  //
  // SlidingDes
  //

  case class SlidingDes(
      expr: TimeSeriesExpr,
      trainingSize: Int,
      alpha: Double,
      beta: Double) extends StatefulExpr {
    import com.netflix.atlas.core.model.StatefulExpr.SlidingDes._

    def dataExprs: List[DataExpr] = expr.dataExprs
    override def toString: String = s"$expr,$trainingSize,$alpha,$beta,:sdes"

    def isGrouped: Boolean = expr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

    private def eval(ts: ArrayTimeSeq, s: State): State = {
      var desF = OnlineSlidingDes(s.desState)
      var skipUpTo = s.skipUpTo
      val data = ts.data
      var pos = 0
      while (pos < data.length) {
        if (ts.start + pos * ts.step < skipUpTo) {
          data(pos) = Double.NaN
        } else {
          val yn = data(pos)
          data(pos) = desF.next(yn)
        }
        pos += 1
      }
      State(skipUpTo, desF.state)
    }

    private def getAlignedStartTime(context: EvalContext): Long = {
      val trainingStep = context.step * trainingSize
      if (context.start % trainingStep == 0)
        context.start
      else
        context.start / trainingStep * trainingStep + trainingStep
    }

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      val state = rs.state.getOrElse(this, new StateMap).asInstanceOf[StateMap]
      val newData = rs.data.map { t =>
        val bounded = t.data.bounded(context.start, context.end)
        val s = state.getOrElse(t.id, {
          val alignedStart = getAlignedStartTime(context)
          val desF = OnlineSlidingDes(trainingSize, alpha, beta)
          desF.reset()
          State(alignedStart, desF.state)
        })
        state(t.id) = eval(bounded, s)
        TimeSeries(t.tags, s"sdes(${t.label})", bounded)
      }
      ResultSet(this, newData, rs.state + (this -> state))
    }
  }

  object SlidingDes {
    case class State(skipUpTo: Long, desState: OnlineSlidingDes.State)

    type StateMap = scala.collection.mutable.AnyRefMap[BigInteger, State]
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
      if (period <= 1) rs else {
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

    type StateMap = scala.collection.mutable.AnyRefMap[BigInteger, State]
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

    type StateMap = scala.collection.mutable.AnyRefMap[BigInteger, State]
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

    type StateMap = scala.collection.mutable.AnyRefMap[BigInteger, State]
  }

}
