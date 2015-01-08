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

import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.temporal.ChronoField

import com.netflix.atlas.core.util.Math

trait MathExpr extends TimeSeriesExpr

object MathExpr {
  case class Constant(v: Double) extends TimeSeriesExpr {
    def dataExprs: List[DataExpr] = Nil
    override def toString: String = s"$v,:const"

    def isGrouped: Boolean = false

    def groupByKey(tags: Map[String, String]): Option[String] = None

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val seq = new FunctionTimeSeq(DsType.Gauge, context.step, _ => v)
      val ts = TimeSeries(Map("name" -> v.toString), v.toString, seq)
      ResultSet(this, List(ts), context.state)
    }
  }

  case object Random extends TimeSeriesExpr {
    def dataExprs: List[DataExpr] = Nil
    override def toString: String = s":random"

    def isGrouped: Boolean = false

    def groupByKey(tags: Map[String, String]): Option[String] = None

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val seq = new FunctionTimeSeq(DsType.Gauge, context.step, rand)
      val ts = TimeSeries(Map("name" -> "random"), "random", seq)
      ResultSet(this, List(ts), context.state)
    }

    private def rand(t: Long): Double = {
      scala.util.Random.nextDouble()
    }
  }

  case class Time(mode: String) extends TimeSeriesExpr {

    private val chronoField = mode match {
      case "secondOfMinute" => ChronoField.SECOND_OF_MINUTE
      case "secondOfDay"    => ChronoField.SECOND_OF_DAY
      case "minuteOfHour"   => ChronoField.MINUTE_OF_HOUR
      case "minuteOfDay"    => ChronoField.MINUTE_OF_DAY
      case "hourOfDay"      => ChronoField.HOUR_OF_DAY
      case "dayOfWeek"      => ChronoField.DAY_OF_WEEK
      case "dayOfMonth"     => ChronoField.DAY_OF_MONTH
      case "dayOfYear"      => ChronoField.DAY_OF_YEAR
      case "monthOfYear"    => ChronoField.MONTH_OF_YEAR
      case "yearOfCentury"  => ChronoField.YEAR
      case "yearOfEra"      => ChronoField.YEAR_OF_ERA
      case "seconds"        => ChronoField.INSTANT_SECONDS
      case "minutes"        => ChronoField.INSTANT_SECONDS
      case "hours"          => ChronoField.INSTANT_SECONDS
      case "days"           => ChronoField.INSTANT_SECONDS
      case "weeks"          => ChronoField.INSTANT_SECONDS
      case s                => ChronoField.valueOf(s)
    }

    private val valueFunc = {
      if (chronoField != ChronoField.INSTANT_SECONDS) usingCalendar _ else {
        mode match {
          case "seconds" => sinceEpoch(1000L) _
          case "minutes" => sinceEpoch(1000L * 60L) _
          case "hours"   => sinceEpoch(1000L * 60L * 60L) _
          case "days"    => sinceEpoch(1000L * 60L * 60L * 24L) _
          case "weeks"   => sinceEpoch(1000L * 60L * 60L * 24L * 7L) _
        }
      }
    }

    private def usingCalendar(t: Long): Double = {
      ZonedDateTime.ofInstant(Instant.ofEpochMilli(t), ZoneOffset.UTC).get(chronoField)
    }

    private def sinceEpoch(divisor: Long)(t: Long): Double = t / divisor

    def dataExprs: List[DataExpr] = Nil
    override def toString: String = s"$mode,:time"

    def isGrouped: Boolean = false

    def groupByKey(tags: Map[String, String]): Option[String] = None

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val seq = new FunctionTimeSeq(DsType.Gauge, context.step, valueFunc)
      val ts = TimeSeries(Map("name" -> mode), mode, seq)
      ResultSet(this, List(ts), context.state)
    }
  }

  trait UnaryMathExpr extends TimeSeriesExpr with UnaryOp {
    def name: String
    def expr: TimeSeriesExpr
    def dataExprs: List[DataExpr] = expr.dataExprs
    override def toString: String = s"$expr,:$name"

    def isGrouped: Boolean = expr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      ResultSet(this, rs.data.map { t => t.unaryOp(s"$name(%s)", this) }, rs.state)
    }
  }

  case class Abs(expr: TimeSeriesExpr) extends UnaryMathExpr {
    def name: String = "abs"
    def apply(v: Double): Double = math.abs(v)
  }

  case class Negate(expr: TimeSeriesExpr) extends UnaryMathExpr {
    def name: String = "neg"
    def apply(v: Double): Double = -v
  }

  case class Sqrt(expr: TimeSeriesExpr) extends UnaryMathExpr {
    def name: String = "sqrt"
    def apply(v: Double): Double = math.sqrt(v)
  }

  case class PerStep(expr: TimeSeriesExpr) extends UnaryMathExpr {
    def name: String = "per-step"

    // Not used, required by base-class
    def apply(v: Double): Double = v

    override def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      val newData = rs.data.map { t =>
        // Assumes rate-per-second counter
        val multiple = t.data.step / 1000
        t.unaryOp(s"$name(%s)", v => v * multiple)
      }
      ResultSet(this, newData, rs.state)
    }
  }

  trait BinaryMathExpr extends TimeSeriesExpr with BinaryOp {
    def name: String
    def labelFmt: String
    def expr1: TimeSeriesExpr
    def expr2: TimeSeriesExpr
    def dataExprs: List[DataExpr] = expr1.dataExprs ::: expr2.dataExprs
    override def toString: String = s"$expr1,$expr2,:$name"

    def isGrouped: Boolean = expr1.isGrouped || expr2.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = {
      expr1.groupByKey(tags).orElse(expr2.groupByKey(tags))
    }

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs1 = expr1.eval(context, data)
      val rs2 = expr2.eval(context, data)
      val result = (expr1.isGrouped, expr2.isGrouped) match {
        case (_, false) =>
          require(rs2.data.size == 1)
          val t2 = rs2.data.head
          rs1.data.map(_.binaryOp(t2, labelFmt, this))
        case (false, _) =>
          require(rs1.data.size == 1)
          val t1 = rs1.data.head
          // Normally tags are kept for the lhs, in this case we want to prefer the tags from
          // the grouped expr on the rhs
          rs2.data.map(t2 => t1.binaryOp(t2, labelFmt, this).withTags(t2.tags))
        case (true, true) =>
          val g2 = rs2.data.groupBy(t => groupByKey(t.tags))
          rs1.data.flatMap { t1 =>
            val k = groupByKey(t1.tags)
            g2.get(k).map {
              case t2 :: Nil => t1.binaryOp(t2, labelFmt, this)
              case _         => throw new IllegalStateException("too many values for key")
            }
          }
      }
      ResultSet(this, result, rs1.state ++ rs2.state)
    }
  }

  case class Add(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {
    def name: String = "add"
    def labelFmt: String = "(%s + %s)"
    def apply(v1: Double, v2: Double): Double = Math.addNaN(v1, v2)
  }

  case class Subtract(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {
    def name: String = "sub"
    def labelFmt: String = "(%s - %s)"
    def apply(v1: Double, v2: Double): Double = Math.subtractNaN(v1, v2)
  }

  case class Multiply(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {
    def name: String = "mul"
    def labelFmt: String = "(%s * %s)"
    def apply(v1: Double, v2: Double): Double = v1 * v2
  }

  case class Divide(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {
    def name: String = "div"
    def labelFmt: String = "(%s / %s)"
    def apply(v1: Double, v2: Double): Double = if (v2 == 0.0) Double.NaN else v1 / v2
  }

  case class GreaterThan(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {
    def name: String = "gt"
    def labelFmt: String = "(%s > %s)"
    def apply(v1: Double, v2: Double): Double = Math.gtNaN(v1, v2)
  }

  case class GreaterThanEqual(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {
    def name: String = "ge"
    def labelFmt: String = "(%s >= %s)"
    def apply(v1: Double, v2: Double): Double = if (v1 >= v2) 1.0 else 0.0
  }

  case class LessThan(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {
    def name: String = "lt"
    def labelFmt: String = "(%s < %s)"
    def apply(v1: Double, v2: Double): Double = Math.ltNaN(v1, v2)
  }

  case class LessThanEqual(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {
    def name: String = "le"
    def labelFmt: String = "(%s <= %s)"
    def apply(v1: Double, v2: Double): Double = if (v1 <= v2) 1.0 else 0.0
  }

  case class FAdd(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {
    def name: String = "fadd"
    def labelFmt: String = "(%s + %s)"
    def apply(v1: Double, v2: Double): Double = v1 + v2
  }

  case class FSubtract(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {
    def name: String = "fsub"
    def labelFmt: String = "(%s - %s)"
    def apply(v1: Double, v2: Double): Double = v1 - v2
  }

  case class FMultiply(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {
    def name: String = "fmul"
    def labelFmt: String = "(%s * %s)"
    def apply(v1: Double, v2: Double): Double = v1 * v2
  }

  case class FDivide(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {
    def name: String = "fdiv"
    def labelFmt: String = "(%s / %s)"
    def apply(v1: Double, v2: Double): Double = if (v2 == 0.0) Double.NaN else v1 / v2
  }

  case class And(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {
    def name: String = "and"
    def labelFmt: String = "(%s AND %s)"
    def apply(v1: Double, v2: Double): Double = {
      if (Math.toBoolean(v1) && Math.toBoolean(v2)) 1.0 else 0.0
    }
  }

  case class Or(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {
    def name: String = "or"
    def labelFmt: String = "(%s OR %s)"
    def apply(v1: Double, v2: Double): Double = {
      if (Math.toBoolean(v1) || Math.toBoolean(v2)) 1.0 else 0.0
    }
  }

  trait AggrMathExpr extends TimeSeriesExpr with BinaryOp {
    def name: String
    def expr: TimeSeriesExpr
    def dataExprs: List[DataExpr] = expr.dataExprs
    override def toString: String = s"$expr,:$name"

    def isGrouped: Boolean = false

    def groupByKey(tags: Map[String, String]): Option[String] = None

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      val t = TimeSeries.aggregate(rs.data.iterator, context.start, context.end, this)
      ResultSet(this, List(TimeSeries(t.tags, s"$name(${t.label})", t.data)), rs.state)
    }
  }

  case class Sum(expr: TimeSeriesExpr) extends AggrMathExpr {
    def name: String = "sum"
    def apply(v1: Double, v2: Double): Double = Math.addNaN(v1, v2)
  }

  case class Count(expr: TimeSeriesExpr) extends AggrMathExpr {
    def name: String = "count"
    def apply(v1: Double, v2: Double): Double = Math.addNaN(v1, v2)

    override def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      val init = rs.data.map { t =>
        TimeSeries(t.tags, t.label, t.data.mapValues(v => if (v.isNaN) Double.NaN else 1.0))
      }
      val t = TimeSeries.aggregate(init.iterator, context.start, context.end, this)
      ResultSet(this, List(TimeSeries(t.tags, s"$name(${t.label})", t.data)), rs.state)
    }
  }

  case class Min(expr: TimeSeriesExpr) extends AggrMathExpr {
    def name: String = "min"
    def apply(v1: Double, v2: Double): Double = Math.minNaN(v1, v2)
  }

  case class Max(expr: TimeSeriesExpr) extends AggrMathExpr {
    def name: String = "max"
    def apply(v1: Double, v2: Double): Double = Math.maxNaN(v1, v2)
  }

}


