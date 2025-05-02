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
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.temporal.ChronoField
import com.netflix.atlas.core.model.DataExpr.AggregateFunction
import com.netflix.atlas.core.stacklang.Context
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.util.ArrayHelper
import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.Math
import com.netflix.atlas.core.util.Strings
import com.netflix.spectator.api.histogram.PercentileBuckets

import scala.collection.immutable.ArraySeq

trait MathExpr extends TimeSeriesExpr

object MathExpr {

  /**
    * Map a tag key name to an alternate name.
    *
    * @param expr
    *     Input expression to act on.
    * @param original
    *     Original tag name that should be replaced.
    * @param replacement
    *     Replacement tag name that should be used going forward.
    */
  case class As(expr: TimeSeriesExpr, original: String, replacement: String)
      extends TimeSeriesExpr {

    def dataExprs: List[DataExpr] = expr.dataExprs

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, original, replacement, Interpreter.WordToken(":as"))
    }

    def isGrouped: Boolean = expr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = {
      Option(DataExpr.keyString(finalGrouping, tags))
    }

    val finalGrouping: List[String] = {
      expr.finalGrouping.map { k =>
        if (k == original) replacement else k
      }
    }

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      ResultSet(
        this,
        rs.data.map { ts =>
          val tags = ts.tags.get(original) match {
            case Some(v) => ts.tags - original + (replacement -> v)
            case None    => ts.tags
          }
          ts.withTags(tags)
        },
        rs.state
      )
    }
  }

  case class Constant(v: Double) extends TimeSeriesExpr {

    def dataExprs: List[DataExpr] = Nil

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, v, Interpreter.WordToken(":const"))
    }

    def isGrouped: Boolean = false

    def groupByKey(tags: Map[String, String]): Option[String] = None

    def finalGrouping: List[String] = Nil

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val seq = new FunctionTimeSeq(DsType.Gauge, context.step, _ => v)
      val ts = TimeSeries(Map("name" -> v.toString), v.toString, seq)
      ResultSet(this, List(ts), context.state)
    }
  }

  /**
    * Generate a time series that appears to be random noise for the purposes of
    * experimentation and generating sample data. To ensure that the line is deterministic
    * and reproducible it actually is based on a hash of the timestamp.
    */
  case object Random extends TimeSeriesExpr {

    def dataExprs: List[DataExpr] = Nil

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, Interpreter.WordToken(":random"))
    }

    def isGrouped: Boolean = false

    def groupByKey(tags: Map[String, String]): Option[String] = None

    def finalGrouping: List[String] = Nil

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val seq = new FunctionTimeSeq(DsType.Gauge, context.step, rand)
      val ts = TimeSeries(Map("name" -> "random"), "random", seq)
      ResultSet(this, List(ts), context.state)
    }

    def rand(t: Long): Double = {
      // Compute the hash and map the value to the range 0.0 to 1.0
      (math.abs(Hash.lowbias64(t)) % 1000) / 1000.0
    }
  }

  /**
    * Same as [Random], but allows the user to specify a seed to vary the input. This allows
    * multiple sample lines to be produced with different values.
    */
  case class SeededRandom(seed: Int) extends TimeSeriesExpr {

    def dataExprs: List[DataExpr] = Nil

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, seed, Interpreter.WordToken(":srandom"))
    }

    def isGrouped: Boolean = false

    def groupByKey(tags: Map[String, String]): Option[String] = None

    def finalGrouping: List[String] = Nil

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val seq = new FunctionTimeSeq(DsType.Gauge, context.step, rand)
      val label = s"seeded-random($seed)"
      val ts = TimeSeries(Map("name" -> label), label, seq)
      ResultSet(this, List(ts), context.state)
    }

    def rand(t: Long): Double = Random.rand(t ^ seed)
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
      if (chronoField != ChronoField.INSTANT_SECONDS) usingCalendar _
      else {
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

    private def sinceEpoch(divisor: Long)(t: Long): Double = t.toDouble / divisor

    def dataExprs: List[DataExpr] = Nil

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, mode, Interpreter.WordToken(":time"))
    }

    def isGrouped: Boolean = false

    def groupByKey(tags: Map[String, String]): Option[String] = None

    def finalGrouping: List[String] = Nil

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val seq = new FunctionTimeSeq(DsType.Gauge, context.step, valueFunc)
      val ts = TimeSeries(Map("name" -> mode), mode, seq)
      ResultSet(this, List(ts), context.state)
    }
  }

  case class TimeSpan(s: String, e: String, zone: ZoneId) extends TimeSeriesExpr {

    def dataExprs: List[DataExpr] = Nil

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, s, e, Interpreter.WordToken(":time-span"))
    }

    def isGrouped: Boolean = false

    def groupByKey(tags: Map[String, String]): Option[String] = None

    def finalGrouping: List[String] = Nil

    private def parseDates(context: EvalContext): (ZonedDateTime, ZonedDateTime) = {
      val gs = Instant.ofEpochMilli(context.start).atZone(zone)
      val ge = Instant.ofEpochMilli(context.end).atZone(zone)

      val sref = Strings.extractReferencePointDate(s)
      val eref = Strings.extractReferencePointDate(e)

      // Sanity check that the relative dates are sane
      if (sref.contains("e") && eref.contains("s")) {
        throw new IllegalArgumentException("start and end time are relative to each other")
      }
      if (sref.contains("s")) {
        throw new IllegalArgumentException("start time is relative to itself")
      }
      if (eref.contains("e")) {
        throw new IllegalArgumentException("end time is relative to itself")
      }

      val refs = Map("gs" -> gs, "ge" -> ge)

      // If one is relative to the other, the absolute date must be computed first
      if (sref.contains("e")) {
        // start time is relative to end time
        val end = Strings.parseDate(e, zone, refs)
        val start = Strings.parseDate(s, zone, refs + ("e" -> end))
        start -> end
      } else if (eref.contains("s")) {
        // end time is relative to start time
        val start = Strings.parseDate(s, zone, refs)
        val end = Strings.parseDate(e, zone, refs + ("s" -> start))
        start -> end
      } else {
        val start = Strings.parseDate(s, zone, refs)
        val end = Strings.parseDate(e, zone, refs)
        start -> end
      }
    }

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {

      val (start, end) = parseDates(context)

      val startMillis = start.toInstant.toEpochMilli
      val endMillis = end.toInstant.toEpochMilli

      require(startMillis <= endMillis, "start must be <= end")

      def contains(t: Long): Double = {
        if (startMillis <= t && t <= endMillis) 1.0 else 0.0
      }

      val seq = new FunctionTimeSeq(DsType.Gauge, context.step, contains)
      val ts = TimeSeries(Map("name" -> s"$s to $e"), s"$s to $e", seq)
      ResultSet(this, List(ts), context.state)
    }
  }

  case class ClampMin(expr: TimeSeriesExpr, min: Double) extends TimeSeriesExpr with UnaryOp {

    def name: String = "clamp-min"

    def dataExprs: List[DataExpr] = expr.dataExprs

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, min, Interpreter.WordToken(s":$name"))
    }

    def isGrouped: Boolean = expr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

    def finalGrouping: List[String] = expr.finalGrouping

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      ResultSet(
        this,
        rs.data.map { t =>
          t.unaryOp(s"$name(%s, $min)", this)
        },
        rs.state
      )
    }

    def apply(v: Double): Double = if (v < min) min else v
  }

  case class ClampMax(expr: TimeSeriesExpr, max: Double) extends TimeSeriesExpr with UnaryOp {

    def name: String = "clamp-max"

    def dataExprs: List[DataExpr] = expr.dataExprs

    override def toString: String = Interpreter.toString(expr, max, s":$name")

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, max, Interpreter.WordToken(s":$name"))
    }

    def isGrouped: Boolean = expr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

    def finalGrouping: List[String] = expr.finalGrouping

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      ResultSet(
        this,
        rs.data.map { t =>
          t.unaryOp(s"$name(%s, $max)", this)
        },
        rs.state
      )
    }

    def apply(v: Double): Double = if (v > max) max else v
  }

  trait UnaryMathExpr extends TimeSeriesExpr with UnaryOp {

    def name: String

    def expr: TimeSeriesExpr

    def dataExprs: List[DataExpr] = expr.dataExprs

    override def toString: String = {
      // Needs to be overridden here or it will get the copy from the based Function type
      val builder = new java.lang.StringBuilder()
      append(builder)
      builder.toString
    }

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, Interpreter.WordToken(s":$name"))
    }

    def isGrouped: Boolean = expr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

    def finalGrouping: List[String] = expr.finalGrouping

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      ResultSet(
        this,
        rs.data.map { t =>
          t.unaryOp(s"$name(%s)", this)
        },
        rs.state
      )
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

  case class Sine(expr: TimeSeriesExpr) extends UnaryMathExpr {

    def name: String = "sin"

    def apply(v: Double): Double = math.sin(v)
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
        // Assumes rate-per-second counter. If the step size is less than a second, then it
        // will be a fractional multiple and reduce the amount.
        val multiple = t.data.step / 1000.0
        t.unaryOp(s"$name(%s)", v => v * multiple)
      }
      ResultSet(this, newData, rs.state)
    }
  }

  trait BinaryMathExpr extends TimeSeriesExpr with BinaryOp {

    // Keep the original list to preserve the order in error message
    private[this] val g1 = expr1.finalGrouping
    private[this] val g2 = expr2.finalGrouping

    // Set for checking consistency
    private[this] val s1 = g1.toSet
    private[this] val s2 = g2.toSet

    // Validate grouping is consistent with expectations
    if (expr1.isGrouped && expr2.isGrouped) {
      if (!isSubsetOrEqual(s1, s2)) {
        val detail = s"${g1.mkString("(,", ",", ",)")} ⊈ ${g2.mkString("(,", ",", ",)")}"
        throw new IllegalArgumentException(
          "both sides of binary operation must have the same grouping or one side" +
            s" must be a subset of the other [$detail]"
        )
      }
    }

    // For the final grouping and group by key of the result expression the larger
    // set of keys should be used. This is used to determine which side is the superset.
    private[this] val useLhsGrouping = s2.subsetOf(s1)

    // Based-on grouping, choose the appropriate operation
    private[this] val binaryOp: (ResultSet, ResultSet) => List[TimeSeries] = {
      if (useLhsGrouping) rhsSubset else lhsSubset
    }

    /** Check if s1 equals s2, s1 is a subset of s2, or vice versa. */
    private def isSubsetOrEqual(s1: Set[String], s2: Set[String]): Boolean = {
      s1.subsetOf(s2) || s2.subsetOf(s1)
    }

    def name: String

    def labelFmt: String

    def expr1: TimeSeriesExpr

    def expr2: TimeSeriesExpr

    def dataExprs: List[DataExpr] = expr1.dataExprs ::: expr2.dataExprs

    override def toString: String = {
      // Needs to be overridden here or it will get the copy from the based Function type
      val builder = new java.lang.StringBuilder()
      append(builder)
      builder.toString
    }

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr1, expr2, Interpreter.WordToken(s":$name"))
    }

    def isGrouped: Boolean = expr1.isGrouped || expr2.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = {
      if (useLhsGrouping)
        expr1.groupByKey(tags)
      else
        expr2.groupByKey(tags)
    }

    def finalGrouping: List[String] = {
      if (useLhsGrouping) expr1.finalGrouping else expr2.finalGrouping
    }

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs1 = expr1.eval(context, data)
      val rs2 = expr2.eval(context, data)
      val result = binaryOp(rs1, rs2)
      ResultSet(this, result, rs1.state ++ rs2.state)
    }

    /** LHS grouping keys are subset of the RHS grouping keys. */
    private def lhsSubset(rs1: ResultSet, rs2: ResultSet): List[TimeSeries] = {
      val groupByKeyF = expr1.groupByKey _
      val g1 = rs1.data.groupBy(t => groupByKeyF(t.tags))
      rs2.data.flatMap { t2 =>
        val k = groupByKeyF(t2.tags)
        g1.get(k).map {
          // Normally tags are kept for the lhs, in this case we want to prefer the tags from
          // the grouped expr on the rhs
          case t1 :: Nil => t1.binaryOp(t2, labelFmt, this).withTags(t2.tags)
          case _         => throw new IllegalStateException("too many values for key")
        }
      }
    }

    /** RHS grouping keys are subset of the LHS grouping keys. */
    private def rhsSubset(rs1: ResultSet, rs2: ResultSet): List[TimeSeries] = {
      val groupByKeyF = expr2.groupByKey _
      val g2 = rs2.data.groupBy(t => groupByKeyF(t.tags))
      rs1.data.flatMap { t1 =>
        val k = groupByKeyF(t1.tags)
        g2.get(k).map {
          case t2 :: Nil => t1.binaryOp(t2, labelFmt, this)
          case _         => throw new IllegalStateException("too many values for key")
        }
      }
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

    def apply(v1: Double, v2: Double): Double = {
      if (v2 == 0.0) {
        // Infinite is not very useful as a value in a visualization and tends to make other
        // values difficult to see. So normally a divide by 0 will report a value of NaN so the
        // user can visually see the value is unknown/misbehaving for that time.
        //
        // However, if the numerator is also 0, this likely means no activity on the counters
        // during a given interval. Reporting NaN can become confusing as users think there is an
        // issue with the data not getting reported. If both are 0 we report 0 as it tends to
        // convey the intent that we received data, but there was no activity rather than we
        // failed to receive any data.
        //
        // The :fdiv operator can be used if a strict floating point division is actually
        // desirable.
        if (v1 == 0.0) 0.0 else Double.NaN
      } else {
        v1 / v2
      }
    }
  }

  case class Power(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {

    def name: String = "pow"

    def labelFmt: String = "pow(%s, %s)"

    def apply(v1: Double, v2: Double): Double = {
      // we just use the behavior of math.pow, so expressions like math.pow(0, 0)
      // or math.pow(Double.PositiveInfinity, 0) will return 1, not NaN (or an arithmetic exception)
      // even though they're technically undefined.

      math.pow(v1, v2)
    }
  }

  case class GreaterThan(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {

    def name: String = "gt"

    def labelFmt: String = "(%s > %s)"

    def apply(v1: Double, v2: Double): Double = if (v1 > v2) 1.0 else 0.0
  }

  case class GreaterThanEqual(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {

    def name: String = "ge"

    def labelFmt: String = "(%s >= %s)"

    def apply(v1: Double, v2: Double): Double = if (v1 >= v2) 1.0 else 0.0
  }

  case class LessThan(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends BinaryMathExpr {

    def name: String = "lt"

    def labelFmt: String = "(%s < %s)"

    def apply(v1: Double, v2: Double): Double = if (v1 < v2) 1.0 else 0.0
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

    def apply(v1: Double, v2: Double): Double = v1 / v2
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

  sealed trait AggrMathExpr extends TimeSeriesExpr {

    def name: String

    def expr: TimeSeriesExpr

    def aggregator(start: Long, end: Long): TimeSeries.Aggregator

    def dataExprs: List[DataExpr] = expr.dataExprs

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, Interpreter.WordToken(s":$name"))
    }

    def isGrouped: Boolean = false

    def groupByKey(tags: Map[String, String]): Option[String] = None

    def finalGrouping: List[String] = Nil

    /**
      * Returns the set of tag keys that should be present on time series evaluated by
      * this expression. This will be the exact set in the first data expression.
      */
    private def filterKeys(tags: Map[String, String]): Map[String, String] = dataExprs match {
      case dataExpr :: _ =>
        val keys = Query.exactKeys(dataExpr.query)
        tags.filter(t => keys.contains(t._1))
      case _ =>
        tags
    }

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      val ts =
        if (rs.data.isEmpty) {
          List(TimeSeries.noData(context.step))
        } else {
          val tags = filterKeys(rs.data.head.tags)
          val label = TimeSeries.toLabel(tags)
          val aggr = aggregator(context.start, context.end)
          rs.data.foreach(aggr.update)
          val t = aggr.result()
          List(TimeSeries(tags, s"$name($label)", t.data))
        }
      ResultSet(this, ts, rs.state)
    }
  }

  case class Sum(expr: TimeSeriesExpr) extends AggrMathExpr {

    override def name: String = "sum"

    override def aggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      new TimeSeries.SimpleAggregator(start, end, Math.addNaN)
    }
  }

  case class Count(expr: TimeSeriesExpr) extends AggrMathExpr {

    override def name: String = "count"

    override def aggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      new TimeSeries.CountAggregator(start, end)
    }
  }

  case class Min(expr: TimeSeriesExpr) extends AggrMathExpr {

    override def name: String = "min"

    override def aggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      new TimeSeries.SimpleAggregator(start, end, Math.minNaN)
    }
  }

  case class Max(expr: TimeSeriesExpr) extends AggrMathExpr {

    override def name: String = "max"

    override def aggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      new TimeSeries.SimpleAggregator(start, end, Math.maxNaN)
    }
  }

  case class GroupBy(expr: AggrMathExpr, keys: List[String]) extends TimeSeriesExpr {

    // This variant should only be used if the data is already grouped and we are adding another
    // level. See DataExpr.GroupBy for the first level based on the raw data.
    require(expr.expr.isGrouped, "input expression must already be grouped with DataExpr.GroupBy")

    // We can only group using a subset of the previous group by results based on the final
    // grouping before the preceding aggregate.
    private val preAggrGrouping = expr.expr.finalGrouping

    require(
      keys.forall(preAggrGrouping.contains),
      s"(,${keys.mkString(",")},) is not a subset of (,${preAggrGrouping.mkString(",")},)"
    )

    // Extract the common keys from queries so we can retain those tags in in the final output
    // to the user.
    private val queryKeys = {
      val queries = expr.dataExprs.map(_.query)
      if (queries.isEmpty) Set.empty[String]
      else {
        queries.tail.foldLeft(Query.exactKeys(queries.head)) { (acc, q) =>
          acc.intersect(Query.exactKeys(q))
        }
      }
    }

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, keys, Interpreter.WordToken(":by"))
    }

    def dataExprs: List[DataExpr] = expr.dataExprs

    def isGrouped: Boolean = true

    def groupByKey(tags: Map[String, String]): Option[String] =
      Option(DataExpr.keyString(keys, tags))

    def finalGrouping: List[String] = keys

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val inner = expr.expr.eval(context, data)

      val ks = queryKeys ++ keys
      val groups = inner.data
        .groupBy(t => DataExpr.keyString(keys, t.tags))
        .toList
      val sorted = groups.sortWith(_._1 < _._1)
      val newData = sorted.flatMap {
        case (null, _) => Nil
        case (_, Nil)  => List(TimeSeries.noData(context.step))
        case (k, ts) =>
          val tags = ts.head.tags.filter(e => ks.contains(e._1))
          val aggr = expr.aggregator(context.start, context.end)
          ts.foreach(aggr.update)
          val t = aggr.result()
          List(TimeSeries(tags, k, t.data))
      }

      ResultSet(this, newData, inner.state)
    }
  }

  /**
    * Compute estimated percentile values using counts for well known buckets. See spectator
    * PercentileBuckets for more information. The input will be grouped by the `percentile` key
    * with each key value being the bucket index. The output will be one line per requested
    * percentile.
    *
    * @param expr
    *     Input data expression. The value should be a sum or group by. The group by list should
    *     include the 'percentile' key. If using the `:percentiles` word to construct the instance
    *     then other aggregate types, such as max, will automatically be converted to sum and the
    *     `percentile` key will be added into the group by clause.
    * @param percentiles
    *     List of percentiles to compute. Each value should be in the range [0.0, 100.0].
    */
  case class Percentiles(expr: DataExpr.GroupBy, percentiles: List[Double]) extends TimeSeriesExpr {

    require(
      expr.keys.contains(TagKey.percentile),
      s"key list for group by must contain '${TagKey.percentile}'"
    )

    percentiles.foreach { p =>
      require(p >= 0.0 && p <= 100.0, s"invalid percentile $p, value must be 0.0 <= p <= 100.0")
    }

    private val evalGroupKeys = expr.keys.filter(_ != TagKey.percentile)
    private val pcts = percentiles.distinct.sortWith(_ < _).toArray

    override def append(builder: java.lang.StringBuilder): Unit = {
      // Base expr
      if (evalGroupKeys.nonEmpty)
        Interpreter.append(builder, expr.af.query, evalGroupKeys, Interpreter.WordToken(":by"))
      else
        Interpreter.append(builder, expr.af.query)

      // Percentiles
      builder.append(",(,")
      Interpreter.append(builder, ArraySeq.unsafeWrapArray(pcts)*)
      builder.append(",),:percentiles")

      // Offset
      if (!expr.offset.isZero)
        builder.append(',').append(expr.offset).append(",:offset")
    }

    override def dataExprs: List[DataExpr] = List(expr)

    override def isGrouped: Boolean = true

    override def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

    def finalGrouping: List[String] = expr.finalGrouping

    override def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val inner = expr.eval(context, data)
      if (inner.data.isEmpty) {
        inner
      } else if (evalGroupKeys.isEmpty) {
        val label = expr.af.query.labelString
        ResultSet(this, estimatePercentiles(context, label, inner.data), context.state)
      } else {
        val groups = inner.data.groupBy(_.tags - TagKey.percentile)
        val rs = groups.values.toList.flatMap { ts =>
          if (ts.isEmpty) ts
          else {
            val tags = ts.head.tags - TagKey.percentile
            val label = DataExpr.keyString(evalGroupKeys, tags)
            estimatePercentiles(context, label, ts)
          }
        }
        ResultSet(this, rs, context.state)
      }
    }

    private def estimatePercentiles(
      context: EvalContext,
      baseLabel: String,
      data: List[TimeSeries]
    ): List[TimeSeries] = {

      // If the mapping on top of the data layer puts in a "no data" time series as a
      // placeholder, then there will be entries without the percentile tag. Ideally
      // it would be fixed at the data layer, but this check provides a better user
      // experience otherwise as it will not fail all together.
      val filtered = data.filter(_.tags.contains(TagKey.percentile))
      if (filtered.isEmpty) {
        List(TimeSeries.noData(context.step))
      } else {
        val length = ((context.end - context.start) / context.step).toInt

        // Output time sequences, one for each output percentile we need to estimate
        val output = Array.fill[ArrayTimeSeq](pcts.length) {
          val buf = ArrayHelper.fill(length, Double.NaN)
          new ArrayTimeSeq(DsType.Gauge, context.start, context.step, buf)
        }

        // Count for each bucket
        val counts = new Array[Double](PercentileBuckets.length())
        val byBucket = filtered.groupBy { t =>
          // Value should have a prefix of T or D, followed by 4 digit hex integer indicating the
          // bucket index
          val idx = t.tags(TagKey.percentile).substring(1)
          Integer.parseInt(idx, 16)
        }

        // Counts that are actually present in the input
        val usedCounts = byBucket.keys.toArray
        java.util.Arrays.sort(usedCounts)

        // Input sequences
        val bounded = new Array[ArrayTimeSeq](usedCounts.length)
        var i = 0
        while (i < usedCounts.length) {
          val vs = byBucket(usedCounts(i))
          require(
            vs.lengthCompare(1) == 0,
            s"invalid percentile encoding: [${vs.map(_.tags(TagKey.percentile)).mkString(",")}]"
          )
          bounded(i) = vs.head.data.bounded(context.start, context.end)
          i += 1
        }

        // Array percentile results will get written to
        val results = new Array[Double](pcts.length)

        // If the input was a timer the unit for the buckets is nanoseconds. The type is reflected
        // by the prefix of T on the bucket key. After estimating the value we multiply by 1e-9 to
        // keep the result in a base unit of seconds.
        val isTimer = filtered.head.tags(TagKey.percentile).startsWith("T")
        val cnvFactor = if (isTimer) 1e-9 else 1.0

        // Loop across each time interval. This section is the tight loop so we keep it as simple
        // array accesses and basic loops to minimize performance overhead.
        i = 0
        while (i < length) {
          // Fill in the counts for this interval and compute the estimate
          var j = 0
          while (j < usedCounts.length) {
            val v = bounded(j).data(i)
            counts(usedCounts(j)) = if (v.isFinite) v else 0.0
            j += 1
          }
          PercentileBuckets.percentiles(counts, pcts, results)

          // Fill in the output sequences with the results
          j = 0
          while (j < results.length) {
            output(j).data(i) = results(j) * cnvFactor
            j += 1
          }
          i += 1
        }

        // Apply the tags and labels to the output. The percentile values are padded with a
        // space so that the decimal place will line up vertically when using a monospace font
        // for rendering.
        output.toList.zipWithIndex.map {
          case (seq, j) =>
            val p = pcts(j) match {
              case v if v < 10.0  => s"  $v"
              case v if v < 100.0 => s" $v"
              case v              => v.toString
            }
            val tags = data.head.tags + (TagKey.percentile -> p)
            TimeSeries(tags, f"percentile($baseLabel, $p)", seq)
        }
      }
    }
  }

  /**
    * Named rewrites are used to keep track of the user intent for operations and
    * macros that are defined in terms of other basic operations. For example, `:avg`
    * is not available as a basic aggregate type, it is a rewrite to
    * `query,:sum,query,:count,:div`. However, for the user it is better if we can
    * show `query,:avg` when dumping the expression as a string.
    *
    * @param name
    *     Name of the operation, e.g., `avg`.
    * @param displayExpr
    *     Expression that is displayed to the user when creating the expression string.
    * @param evalExpr
    *     Expression that is evaluated.
    * @param context
    *     Evaluation context for the initial creation time. This context is used to
    *     re-evaluate the rewrite using the original context if the overall expression
    *     is rewritten (`Expr.rewrite()`) later.
    */
  case class NamedRewrite(
    name: String,
    displayExpr: Expr,
    displayParams: List[Any],
    evalExpr: TimeSeriesExpr,
    context: Context,
    groupByRewrite: Option[(Expr, List[String]) => Expr] = None
  ) extends TimeSeriesExpr {

    def dataExprs: List[DataExpr] = evalExpr.dataExprs

    override def append(builder: java.lang.StringBuilder): Unit = {
      append(builder, displayExpr)
    }

    private def append(builder: java.lang.StringBuilder, expr: Expr): Unit = {
      val op =
        if (displayParams.isEmpty)
          s":$name"
        else
          displayParams.mkString("", ",", s",:$name")
      expr match {
        case q: Query =>
          // If the displayExpr is a query type, then the rewrite is simulating an
          // aggregate function. Modifications to the aggregate need to be represented
          // after the operation as part of the expression string. There are two
          // categories: offsets applied to the data function and group by.
          builder.append(s"$q,$op")
          getOffset(evalExpr).foreach(d => builder.append(s",$d,:offset"))

          val grouping = evalExpr.finalGrouping
          if (grouping.nonEmpty) {
            builder.append(grouping.mkString(",(,", ",", ",),:by"))
          }
        case t: TimeSeriesExpr if groupingMatches =>
          // The passed in expression maybe the result of a rewrite to the display expression
          // that was not applied to the eval expression. If it changes the grouping, then it
          // would alter the toString behavior. So the grouping match check is based on the
          // original display expression.
          val evalOffset = getOffset(evalExpr)
          val str = evalOffset.fold(s"$t,$op") { d =>
            val displayOffset = getOffset(t).getOrElse(Duration.ZERO)
            if (d != displayOffset) s"${t.withOffset(d)},$op" else s"$t,$op"
          }
          builder.append(str)
        case _ =>
          Interpreter.append(builder, expr, Interpreter.WordToken(op))
          val grouping = evalExpr.finalGrouping
          if (grouping.nonEmpty) {
            builder.append(",(,")
            Interpreter.append(builder, grouping*)
            builder.append(",),:by")
          }
      }
    }

    private def toString(expr: Expr): String = {
      val builder = new java.lang.StringBuilder()
      append(builder, expr)
      builder.toString
    }

    private def groupingMatches: Boolean = {
      displayExpr match {
        case t: TimeSeriesExpr => t.finalGrouping == evalExpr.finalGrouping
        case _                 => false
      }
    }

    def isGrouped: Boolean = evalExpr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = evalExpr.groupByKey(tags)

    def finalGrouping: List[String] = evalExpr.finalGrouping

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      evalExpr.eval(context, data).copy(expr = this)
    }

    override def rewrite(f: PartialFunction[Expr, Expr]): Expr = {
      if (f.isDefinedAt(this)) {
        // The partial function is defined for the rewrite itself, assume the caller
        // knows what to do
        super.rewrite(f)
      } else {
        val newDisplayExpr = displayExpr.rewrite(f)
        val ctxt = context.interpreter.execute(toString(newDisplayExpr))
        ctxt.stack match {
          case (r: NamedRewrite) :: Nil => r
          case _ => throw new IllegalStateException(s"invalid stack for :$name")
        }
      }
    }

    def groupBy(keys: List[String]): NamedRewrite = {
      def applyGroupBy: PartialFunction[Expr, Expr] = {
        case af: AggregateFunction => DataExpr.GroupBy(af, keys)
        case af: AggrMathExpr      => MathExpr.GroupBy(af, keys)
      }
      val newDisplayExpr = if (isDisplayGrouped) displayExpr else displayExpr.rewrite(applyGroupBy)

      val newEvalExpr = groupByRewrite
        .fold(evalExpr.rewrite(applyGroupBy)) { f =>
          f(displayExpr, keys)
        }
        .asInstanceOf[TimeSeriesExpr]

      // The display expression may not have the offset encoded, make sure we retain
      // the offset from the eval expression
      val finalEvalExpr = newEvalExpr.withOffset(getOffset(evalExpr).getOrElse(Duration.ZERO))
      copy(displayExpr = newDisplayExpr, evalExpr = finalEvalExpr)
    }

    private def isDisplayGrouped: Boolean = {
      displayExpr match {
        case t: TimeSeriesExpr => t.isGrouped
        case _                 => false
      }
    }

    private def getOffset(expr: TimeSeriesExpr): Option[Duration] = {
      val offsets = expr.dataExprs.map(_.offset).distinct
      offsets match {
        case d :: Nil if !d.isZero => Some(d)
        case _                     => None
      }
    }

    override def withOffset(d: Duration): TimeSeriesExpr = {
      copy(evalExpr = evalExpr.withOffset(d))
    }
  }
}
