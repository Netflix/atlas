/*
 * Copyright 2014-2022 Netflix, Inc.
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
package com.netflix.atlas.chart.graphics

import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import java.time.temporal.ChronoUnit
import com.netflix.atlas.chart.model.Scale
import com.netflix.atlas.core.util.UnitPrefix
import com.netflix.atlas.core.util.UnitPrefix.durationBigPrefixes
import com.netflix.atlas.core.util.UnitPrefix.durationSmallPrefixes
import com.netflix.atlas.core.util.UnitPrefix.sec
import com.netflix.atlas.core.util.UnitPrefix.year

/**
  * Utility for computing the major tick marks to use for a range of values.
  */
object Ticks {

  import java.lang.{Double => JDouble}

  val defaultTimeFmt: DateTimeFormatter = DateTimeFormatter.ofPattern("MMMdd")
  private val monthTimeFmt: DateTimeFormatter = DateTimeFormatter.ofPattern("MMM")
  private val yearTimeFmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy")

  val timeBoundaries = List(
    ChronoField.SECOND_OF_MINUTE -> DateTimeFormatter.ofPattern(":ss"),
    ChronoField.MINUTE_OF_HOUR   -> DateTimeFormatter.ofPattern("HH:mm"),
    ChronoField.HOUR_OF_DAY      -> DateTimeFormatter.ofPattern("HH:mm")
  )

  private def seconds(v: Int): Long = Duration.ofSeconds(v).toMillis

  private def minutes(v: Int): Long = Duration.ofMinutes(v).toMillis

  private def hours(v: Int): Long = Duration.ofHours(v).toMillis

  private def days(v: Int): Long = Duration.ofDays(v).toMillis

  // Major and minor tick sizes for time axis.
  private val timeTickSizes = List(
    minutes(1)  -> seconds(10),
    minutes(2)  -> seconds(30),
    minutes(3)  -> minutes(1),
    minutes(5)  -> minutes(1),
    minutes(10) -> minutes(2),
    minutes(15) -> minutes(5),
    minutes(20) -> minutes(5),
    minutes(30) -> minutes(10),
    hours(1)    -> minutes(10),
    hours(2)    -> minutes(30),
    hours(3)    -> hours(1),
    hours(4)    -> hours(1),
    hours(6)    -> hours(2),
    hours(8)    -> hours(2),
    hours(12)   -> hours(3),
    days(1)     -> hours(4),
    days(2)     -> hours(8),
    days(7)     -> days(1),
    days(2 * 7) -> days(2),
    days(4 * 7) -> days(1 * 7)
  )

  // Major and minor tick sizes for value axis
  private val baseValueTickSizes = List(
    10 -> 2,
    20 -> 5,
    30 -> 10,
    40 -> 10,
    50 -> 10
  )

  // Major and minor tick sizes for value axis
  private val valueTickSizes = (-25 to 25).toList.flatMap { i =>
    val f = math.pow(10, i)
    baseValueTickSizes.map {
      case (major, minor) =>
        val minorPerMajor = major / minor // Number of minor ticks to use between major ticks
        (major * f, minor * f, minorPerMajor)
    }
  }

  // Major and minor tick sizes for value axis
  private val binaryValueTickSizes = {
    val ltOneKi = (-1 to 1).toList.flatMap { i =>
      val f = math.pow(10, i)
      baseValueTickSizes.map {
        case (major, minor) =>
          val minorPerMajor = major / minor // Number of minor ticks to use between major ticks
          (major * f, minor * f, minorPerMajor)
      }
    }

    val majorMultiples = List(1, 2, 3, 4, 5, 10, 20, 30, 40, 50, 100, 200, 300, 400, 500)
    val gtOneKi = UnitPrefix.binaryPrefixes.flatMap { prefix =>
      val n = 4
      val majorF = prefix.factor
      val minorF = prefix.factor / 4.0
      majorMultiples.map { m =>
        (majorF * m, minorF * m, n)
      }
    }

    ltOneKi ::: gtOneKi
  }

  private val durationValueTickSizes = {
    var ticks = (-25 to -2).toList.flatMap { i =>
      val f = math.pow(10, i)
      baseValueTickSizes.map {
        case (major, minor) =>
          val minorPerMajor = major / minor
          (major * f, minor * f, minorPerMajor)
      }
    }

    val majorMultiples = List(
      List(1, 2, 3, 4, 5, 6, 10, 15, 30, 60),
      List(4, 5, 6, 10, 15, 30, 3600),
      List(1, 2, 3, 4, 6, 12, 3600 * 24),
      List(1, 2, 4, 6, 12, 24, 86400 * 7),
      List(1, 2, 3, 4, 6, 86400 * 365)
    )

    var lastFactor = 0.0
    for (i <- 1 until UnitPrefix.durationBigPrefixes.size) {
      val nextPrefix = UnitPrefix.durationBigPrefixes.reverse(i)
      val n = 4
      val multiples = majorMultiples(i - 1)
      val subList = multiples.reverse.map { m =>
        val q = nextPrefix.factor / m
        (lastFactor + q, q / n, n)
      }
      lastFactor = nextPrefix.factor
      ticks = ticks ::: subList
    }

    val mm = List(1, 2, 3, 4, 5, 10, 20, 30, 40, 50, 100, 200, 300, 400, 500)
    val n = 4
    val majorF = year.factor
    val minorF = year.factor / 4.0
    ticks = ticks ::: mm.map { m =>
      (majorF * m, minorF * m, n)
    }
    ticks = ticks ::: (10 to 25).toList.flatMap { i =>
      val f = math.pow(10, i)
      baseValueTickSizes.map {
        case (major, minor) =>
          val minorPerMajor = major / minor
          (major * f, minor * f, minorPerMajor)
      }
    }

    ticks
  }

  /** Round to multiple of `s`. */
  private def round(v: Double, s: Double): Double = s * math.floor(v / s)

  /**
    * Determine the prefix associated with the data. Use the value for the delta between major
    * tick marks if possible. If this will require too many digits to render the largest value, then
    * use the unit prefix found for the largest value.
    */
  private def getPrefix(v: Double, major: Double): UnitPrefix = {
    val m = UnitPrefix.forRange(major, 3)
    if (v <= 10.0 * m.factor) m else UnitPrefix.forRange(v, 3)
  }

  /**
    * Determines the string format pattern to use for showing the label. This is primarily focused
    * on where the decimal point should go such that:
    *
    * 1. All tick labels will have the decimal point in the same position to make visual scanning
    *    easier.
    * 2. Avoid the use of an offset when the number can be shown without an offset by shifting
    *    the decimal point.
    */
  private def labelFormat(prefix: UnitPrefix, v: Double): (String, Double) = {
    val f = v / prefix.factor
    (f * 1000.0).toInt match {
      case i if (i % 10) > 0  => "%.3f%s" -> prefix.factor * 10.0 // 1.234
      case i if (i % 100) > 0 => "%.2f%s" -> prefix.factor * 100.0 // 12.34
      case _                  => "%.1f%s" -> prefix.factor * 1000.0 // 123.4
    }
  }

  /**
    * Determine the prefix associated with the data. Use the value for the delta between major
    * tick marks if possible. If this will require too many digits to render the largest value, then
    * use the unit prefix found for the largest value.
    */
  private def getBinaryPrefix(v: Double, major: Double): UnitPrefix = {
    val m = UnitPrefix.binaryRange(major, 4)
    if (v < 10.0 * m.factor) m else UnitPrefix.binaryRange(v, 4)
  }

  /**
    * Determines the string format pattern to use for showing the label. This is primarily focused
    * on where the decimal point should go such that:
    *
    * 1. All tick labels will have the decimal point in the same position to make visual scanning
    *    easier.
    * 2. Avoid the use of an offset when the number can be shown without an offset by shifting
    *    the decimal point.
    */
  private def binaryLabelFormat(prefix: UnitPrefix, v: Double): (String, Double) = {
    val f = v / prefix.factor
    (f * 1000.0).toInt match {
      case i if (i % 10) > 0  => "%.2f%s" -> prefix.factor * 10.0 // 1.23
      case i if (i % 100) > 0 => "%.1f%s" -> prefix.factor * 100.0 // 12.3
      case _                  => "%.0f%s" -> prefix.factor * 1000.0 //  123
    }
  }

  private def getDurationPrefix(v: Double, major: Double): UnitPrefix = {
    val m = UnitPrefix.durationRange(major)
    if (v <= m.factor) m else UnitPrefix.durationRange(v)
  }

  private def durationLabelFormat(prefix: UnitPrefix, v: Double): String = {
    val f = if (v < 1) v / prefix.factor else v
    (f * 1000.0).toInt match {
      case _ if v >= 3.1536e10 => "%.1e%s" // 1000+ years switch to exponent
      case _ if v >= 60        => "%.0f%s"
      case i if (i % 10) > 0   => "%.3f%s" // 1.234
      case i if (i % 100) > 0  => "%.2f%s" // 12.34
      case _                   => "%.1f%s" // 123.4
    }
  }

  /**
    * Generate value tick marks with approximately `n` major ticks for the range `[v1, v2]`.
    * Uses decimal unit prefixes.
    */
  def value(v1: Double, v2: Double, n: Int, scale: Scale = Scale.LINEAR): List[ValueTick] = {
    val r = validateAndGetRange(v1, v2)

    valueTickSizes
      .find(t => r / t._1 <= n)
      .fold(sciTicks(v1, v2, n))(t => decimalTicks(v1, v2, n, t, scale))
  }

  /**
    * Same as `value(Double,Double,Int)` except that it uses binary unit prefixes.
    */
  def binary(v1: Double, v2: Double, n: Int): List[ValueTick] = {
    val r = validateAndGetRange(v1, v2)

    binaryValueTickSizes
      .find(t => r / t._1 <= n)
      .fold(sciTicks(v1, v2, n))(t => binaryTicks(v1, v2, t))
  }

  def duration(v1: Double, v2: Double, n: Int): List[ValueTick] = {
    val r = validateAndGetRange(v1, v2)

    durationValueTickSizes
      .find(t => r / t._1 <= n)
      .fold(sciTicks(v1, v2, n))(t => durationTicks(v1, v2, t))
  }

  private def validateAndGetRange(v1: Double, v2: Double): Double = {
    require(JDouble.isFinite(v1), "lower bound must be finite")
    require(JDouble.isFinite(v2), "upper bound must be finite")
    require(v1 <= v2, s"v1 must be less than v2 ($v1 > $v2)")
    val range = v2 - v1
    if (range < 1e-12) 1.0 else range
  }

  private def sciTicks(v1: Double, v2: Double, n: Int): List[ValueTick] = {
    List(ValueTick(v1, 0.0), ValueTick(v2, 0.0))
  }

  private def majorLabelDuplication(ticks: List[ValueTick]): Boolean = {
    val majorTicks = ticks.filter(_.major)
    majorTicks.size > majorTicks.map(_.label).distinct.size
  }

  private def decimalTicks(
    v1: Double,
    v2: Double,
    n: Int,
    t: (Double, Double, Int),
    scale: Scale
  ): List[ValueTick] = {
    if (Scale.LOGARITHMIC != scale) {
      return normalTicks(v1, v2, t)
    }

    val logDistanceLimit = 2
    var finalTicks: List[ValueTick] = null

    if (v1 >= 0) {
      // positive range
      val logDistance = logDiff(v1, v2)
      if (logDistance <= logDistanceLimit) {
        return normalTicks(v1, v2, t)
      }
      finalTicks = logScaleTicks(v1, v2, getLogMajorStepSize(logDistance, n))
    } else if (v2 <= 0) {
      // negative range: convert range to pos, generate ticks and convert ticks to negs and reverse
      val logDistance = logDiff(-v2, -v1)
      if (logDistance <= logDistanceLimit) {
        return normalTicks(v1, v2, t)
      }
      finalTicks = toNegTicks(logScaleTicks(-v2, -v1, getLogMajorStepSize(logDistance, n)))
    } else {
      // negative-positive range: split range to pos and neg, get ticks separately and combine
      val posLogDistance = logDiff(0, v2)
      val negLogDistance = logDiff(0, -v1)
      val logDistance = posLogDistance + negLogDistance
      if (posLogDistance <= logDistanceLimit && negLogDistance <= logDistanceLimit) {
        return normalTicks(v1, v2, t)
      }
      val logMajorStepSize = getLogMajorStepSize(logDistance, n)
      val negTicks = toNegTicks(logScaleTicks(0, -v1, logMajorStepSize))
      val posTicks = logScaleTicks(0, v2, logMajorStepSize)
      finalTicks = negTicks.dropRight(1) ++ posTicks // remove the dup 0 tick before combine
    }

    // trim unnecessary ticks
    if (finalTicks.head.v < v1) {
      finalTicks = finalTicks.drop(1)
    }
    if (finalTicks.last.v > v2) {
      finalTicks = finalTicks.dropRight(1)
    }

    finalTicks
  }

  private def normalTicks(v1: Double, v2: Double, t: (Double, Double, Int)): List[ValueTick] = {
    val (major, minor, minorPerMajor) = t
    val ticks = List.newBuilder[ValueTick]

    val prefix = getPrefix(math.abs(v2), major)
    val (labelFmt, maxValue) = labelFormat(prefix, major)

    val base = round(v1, major)
    val end = ((v2 - base) / minor).toInt + 1
    var pos = 0
    while (pos <= end) {
      val v = base + pos * minor
      if (v >= v1 && v <= v2) {
        val label = prefix.format(v, labelFmt)
        ticks += ValueTick(v, 0.0, pos % minorPerMajor == 0, Some(label))
      }
      pos += 1
    }
    val ts = ticks.result()

    val useOffset = majorLabelDuplication(ts)
    if (useOffset) ts.map(t => t.copy(offset = base, labelOpt = None)) else ts
  }

  private def toNegTicks(ticks: List[ValueTick]): List[ValueTick] = {
    ticks.map(t => t.copy(v = -1 * t.v, labelOpt = t.labelOpt.map("-" + _))).reverse
  }

  // Note: all below log* functions are assuming values are non-negative
  private def logScaleTicks(v1: Double, v2: Double, logMajorStepSize: Int): List[ValueTick] = {
    val min = logFloor(v1)
    val max = logCeil(v2)

    val ticks = List.newBuilder[ValueTick]
    var curr = min
    while (curr <= max) {
      // show tick for 0 but not 1(10^0) if lower boundary is 0, because they are too close
      // in log scale
      val v = if (v1 == 0 && curr == 0) 0 else math.pow(10, curr)
      val label = UnitPrefix.format(v, "%.0f%s")
      ticks += ValueTick(v, 0.0, (curr - min) % logMajorStepSize == 0, Some(label))
      curr += 1
    }

    ticks.result()
  }

  private def getLogMajorStepSize(logDistance: Int, n: Int): Int = {
    if (logDistance <= n) {
      1
    } else {
      math.ceil(logDistance / n).toInt
    }
  }

  private def logDiff(v1: Double, v2: Double): Int = {
    require(v1 >= 0, "v1 cannot be negative")
    require(v1 <= v2, "v1 cannot be greater than v2")
    logCeil(v2) - logFloor(v1)
  }

  private def logFloor(v: Double): Int = {
    if (v <= 1) 0 else math.floor(math.log10(v)).toInt
  }

  private def logCeil(v: Double): Int = {
    math.ceil(math.log10(v)).toInt
  }

  private def binaryTicks(v1: Double, v2: Double, t: (Double, Double, Int)): List[ValueTick] = {
    val (major, minor, minorPerMajor) = t
    val ticks = List.newBuilder[ValueTick]

    val prefix = getBinaryPrefix(math.abs(v2), major)
    val (labelFmt, _) = binaryLabelFormat(prefix, major)

    val base = round(v1, major)
    val end = ((v2 - base) / minor).toInt + 1
    var pos = 0
    while (pos <= end) {
      val v = base + pos * minor
      if (v >= v1 && v <= v2) {
        val label = prefix.format(v, labelFmt)
        ticks += ValueTick(v, 0.0, pos % minorPerMajor == 0, Some(label))
      }
      pos += 1
    }
    val ts = ticks.result()

    if (ts.isEmpty) {
      List(ValueTick(v1, v1), ValueTick(v2, v1))
    } else {
      val useOffset = major < math.abs(v1) / 1e2
      if (!useOffset) ts
      else {
        val max = ts.map(t => t.v - base).max
        val offsetPrefix = getBinaryPrefix(max, max)
        val (fmt, _) = binaryLabelFormat(offsetPrefix, max)
        ts.map(t => t.copy(offset = base, labelOpt = Some(offsetPrefix.format(t.v - base, fmt))))
      }
    }
  }

  private def durationTicks(
    v1: Double,
    v2: Double,
    t: (Double, Double, Int),
    prevPrefix: UnitPrefix = null
  ): List[ValueTick] = {
    val (major, minor, minorPerMajor) = t
    val ticks = List.newBuilder[ValueTick]
    val prefix = if (prevPrefix == null) getDurationPrefix(math.abs(v2), major) else prevPrefix
    val labelFmt = durationLabelFormat(prefix, v2)

    val base = round(v1, major)
    val end = ((v2 - base) / minor).toInt + 1
    var pos = 0
    while (pos <= end) {
      val v = base + pos * minor
      if (v >= v1 && v <= v2) {
        val label = prefix.format(v, labelFmt)
        ticks += ValueTick(v, 0.0, pos % minorPerMajor == 0, Some(label))
      }
      pos += 1
    }
    val ts = ticks.result()

    val useOffset = majorLabelDuplication(ts)
    if (useOffset) {
      if (prevPrefix == null && v2 < (year.factor * 2) && v2 > 1e-3) {
        val previousPrefix = prevDurationPrefix(prefix)
        return durationTicks(v1, v2, t, previousPrefix)
      }
      val range = v2 - v1
      val offsetPrefix = getDurationPrefix(range, major)
      val newFormat = durationLabelFormat(prefix, major)

      ts.map(t =>
        t.copy(offset = base, labelOpt = Some(offsetPrefix.format(t.v - base, newFormat)))
      )
    } else ts
  }

  def prevDurationPrefix(prefix: UnitPrefix): UnitPrefix = {
    if (prefix == sec) {
      durationSmallPrefixes.find(_.factor < prefix.factor).getOrElse(prefix)
    } else {
      durationBigPrefixes.find(_.factor < prefix.factor).getOrElse(prefix)
    }
  }

  /**
    * Generate value tick marks with approximately `n` major ticks for the range `[s, e]`. Tick
    * marks will be on significant time boundaries for the specified time zone.
    */
  def time(s: Long, e: Long, zone: ZoneId, n: Int): List[TimeTick] = {

    // To keep even placement of major grid lines the shift amount for the timezone is computed
    // based on the start. If there is a change such as DST during the interval, then labels
    // after the change may be on less significant boundaries.
    val shift = zone.getRules.getOffset(Instant.ofEpochMilli(s)).getTotalSeconds * 1000L

    val dur = e - s
    val candidates = timeTickSizes.filter(t => dur / t._1 <= n)
    if (candidates.nonEmpty) {
      val (major, minor) = candidates.head
      val ticks = List.newBuilder[TimeTick]

      val zs = s + shift
      val ze = e + shift
      var pos = zs / major * major
      while (pos <= ze) {
        if (pos >= zs) ticks += TimeTick(pos - shift, zone, pos % major == 0L)
        pos += minor
      }
      ticks.result()
    } else {
      val start = LocalDateTime.ofInstant(Instant.ofEpochMilli(s), zone).toLocalDate
      val end = LocalDateTime.ofInstant(Instant.ofEpochMilli(e), zone).toLocalDate
      val days = dur / (24 * 60 * 60 * 1000L)
      val (amount, unit, fmt) = days match {
        case d if d <= n       => (1L, ChronoUnit.DAYS, defaultTimeFmt)
        case d if d / 30 <= n  => (1L, ChronoUnit.MONTHS, monthTimeFmt)
        case d if d / 90 <= n  => (3L, ChronoUnit.MONTHS, monthTimeFmt)
        case d if d / 365 <= n => (1L, ChronoUnit.YEARS, yearTimeFmt)
        case d                 => (d / (n * 365), ChronoUnit.YEARS, yearTimeFmt)
      }

      val ticks = List.newBuilder[TimeTick]
      var t = start
      while (t.isBefore(end) || t.isEqual(end)) {
        val timestamp = t.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli - shift
        ticks += TimeTick(timestamp, zone, formatter = Some(fmt))
        t = t.plus(amount, unit)
      }
      ticks.result()
    }
  }

}

/**
  * Tick mark for the value axis.
  *
  * @param v
  *     Value to place the tick mark.
  * @param offset
  *     Offset that will be displayed separately. In some cases if there is a large base value with
  *     a small range of values there will be too many significant digits to show in the tick label
  *     of the axis. If the offset is non-zero, then it should be shown separately and the tick
  *     label should be the difference between the value and the offset.
  * @param major
  *     True if the position is a major tick mark.
  * @param labelOpt
  *     Label to use for the tick mark. If set to None then a default will be generated using
  *     `UnitPrefix`.
  */
case class ValueTick(
  v: Double,
  offset: Double,
  major: Boolean = true,
  labelOpt: Option[String] = None
) {

  def label: String = labelOpt.fold(UnitPrefix.format(v - offset))(v => v)
}

/**
  * Tick mark for the time axis.
  *
  * @param timestamp
  *     Time in milliseconds since the epoch.
  * @param zone
  *     Time zone to use for the string label associated with the timestamp.
  * @param major
  *     True if the position is a major tick mark.
  * @param formatter
  *     Formats the timestamp to a string shown on the axis. If set to None, then a default
  *     will be chosen to try and land on a significant time boundary.
  */
case class TimeTick(
  timestamp: Long,
  zone: ZoneId,
  major: Boolean = true,
  formatter: Option[DateTimeFormatter] = None
) {

  private val datetime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zone)

  import Ticks._

  def label: String = {
    val fmt = formatter.getOrElse {
      timeBoundaries.find(f => datetime.get(f._1) != 0).fold(defaultTimeFmt)(_._2)
    }
    fmt.format(datetime)
  }
}
