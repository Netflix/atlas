/*
 * Copyright 2014-2016 Netflix, Inc.
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

import com.netflix.atlas.core.util.ArrayHelper
import com.netflix.atlas.core.util.Math
import com.netflix.spectator.api.histogram.PercentileBuckets

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

  trait AggrMathExpr extends TimeSeriesExpr with BinaryOp {
    def name: String
    def expr: TimeSeriesExpr
    def dataExprs: List[DataExpr] = expr.dataExprs
    override def toString: String = s"$expr,:$name"

    def isGrouped: Boolean = false

    def groupByKey(tags: Map[String, String]): Option[String] = None

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      val ts = if (rs.data.isEmpty) Nil else {
        val t = TimeSeries.aggregate(rs.data.iterator, context.start, context.end, this)
        List(TimeSeries(t.tags, s"$name(${t.label})", t.data))
      }
      ResultSet(this, ts, rs.state)
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

  case class GroupBy(expr: AggrMathExpr, keys: List[String]) extends TimeSeriesExpr {
    // This variant should only be used if the data is already grouped and we are adding another
    // level. See DataExpr.GroupBy for the first level based on the raw data.
    require(expr.expr.isGrouped, "input expression must already be grouped with DataExpr.GroupBy")

    // We can only group using a subset of the previous group by results.
    private val dataGroups = expr.dataExprs.collect { case e: DataExpr.GroupBy => e }
    dataGroups.foreach { grp =>
      require(grp.keys.containsSlice(keys),
        s"(,${keys.mkString(",")},) is not a subset of (,${grp.keys.mkString(",")},)")
    }

    // Extract the common keys from queries so we can retain those tags in in the final output
    // to the user.
    private val queryKeys = {
      val queries = expr.dataExprs.map(_.query)
      if (queries.isEmpty) Set.empty[String] else {
        queries.tail.foldLeft(Query.exactKeys(queries.head)) { (acc, q) =>
          acc intersect Query.exactKeys(q)
        }
      }
    }

    override def toString: String = s"$expr,(,${keys.mkString(",")},),:by"

    def dataExprs: List[DataExpr] = expr.dataExprs

    def isGrouped: Boolean = true
    def groupByKey(tags: Map[String, String]): Option[String] = Option(DataExpr.keyString(keys, tags))

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val inner = expr.expr.eval(context, data)

      val ks = queryKeys ++ keys
      val groups = inner.data
        .groupBy(t => DataExpr.keyString(keys, t.tags))
        .toList
      val sorted = groups.sortWith(_._1 < _._1)
      val newData = sorted.flatMap {
        case (null, _) => Nil
        case (k, Nil)  => List(TimeSeries.noData(context.step))
        case (k, ts)   =>
          val tags = ts.head.tags.filter(e => ks.contains(e._1))
          val init = expr match {
            case c: Count => ts.map { t =>
              TimeSeries(t.tags, t.label, t.data.mapValues(v => if (v.isNaN) Double.NaN else 1.0))
            }
            case _ => ts
          }
          val t = TimeSeries.aggregate(init.iterator, context.start, context.end, expr)
          List(TimeSeries(tags, k, t.data))
      }

      ResultSet(this, newData, context.state)
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
    require(expr.keys.contains(TagKey.percentile),
      s"key list for group by must contain '${TagKey.percentile}'")

    percentiles.foreach { p =>
      require(p >= 0.0 && p <= 100.0, s"invalid percentile $p, value must be 0.0 <= p <= 100.0")
    }

    private val evalGroupKeys = expr.keys.filter(_ != TagKey.percentile)
    private val pcts = percentiles.distinct.sortWith(_ < _).toArray

    override def toString: String = {
      val baseExpr = if (evalGroupKeys.isEmpty) expr.af.query.toString else {
        s"${expr.af.query},(,${evalGroupKeys.mkString(",")},),:by"
      }
      s"$baseExpr,(,${pcts.mkString(",")},),:percentiles"
    }

    override def dataExprs: List[DataExpr] = List(expr)

    override def isGrouped: Boolean = true

    override def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

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
          if (ts.isEmpty) ts else {
            val tags = ts.head.tags - TagKey.percentile
            val label = DataExpr.keyString(evalGroupKeys, tags)
            estimatePercentiles(context, label, ts)
          }
        }
        ResultSet(this, rs, context.state)
      }
    }

    private def estimatePercentiles(
        context: EvalContext, baseLabel: String, data: List[TimeSeries]): List[TimeSeries] = {
      assert(data.nonEmpty)
      val length = ((context.end - context.start) / context.step).toInt

      // Output time sequences, one for each output percentile we need to estimate
      val output = Array.fill[ArrayTimeSeq](pcts.length) {
        val buf = ArrayHelper.fill(length, Double.NaN)
        new ArrayTimeSeq(DsType.Gauge, context.start, context.step, buf)
      }

      // Count for each bucket
      val counts = new Array[Long](PercentileBuckets.length())
      val byBucket = data.groupBy { t =>
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
        assert(vs.size == 1)
        bounded(i) = vs.head.data.bounded(context.start, context.end)
        i += 1
      }

      // Array percentile results will get written to
      val results = new Array[Double](pcts.length)

      // Inputs are counters reported as a rate per second. We need to convert to a rate per
      // step to get the correct counts for the estimation
      val multiple = context.step / 1000.0

      // If the input was a timer the unit for the buckets is nanoseconds. The type is reflected
      // by the prefix of T on the bucket key. After estimating the value we multiply by 1e-9 to
      // keep the result in a base unit of seconds.
      val isTimer = data.head.tags(TagKey.percentile).startsWith("T")
      val cnvFactor = if (isTimer) 1e-9 else 1.0

      // Loop across each time interval. This section is the tight loop so we keep it as simple
      // array accesses and basic loops to minimize performance overhead.
      i = 0
      while (i < length) {
        // Fill in the counts for this interval and compute the estimate
        var j = 0
        while (j < usedCounts.length) {
          // Note, NaN.toLong == 0, so NaN values are the same as 0 for the count estimate
          val v = (bounded(j).data(i) * multiple).toLong
          counts(usedCounts(j)) = v
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

      // Apply the tags and labels to the output
      output.toList.zipWithIndex.map { case (seq, j) =>
        val p = f"${pcts(j)}%5.1f"
        val tags = data.head.tags + (TagKey.percentile -> p)
        TimeSeries(tags, f"percentile($baseLabel, $p)", seq)
      }
    }
  }
}


