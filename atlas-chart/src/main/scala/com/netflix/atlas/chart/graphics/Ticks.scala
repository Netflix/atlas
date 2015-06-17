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
package com.netflix.atlas.chart.graphics

import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

import com.netflix.atlas.core.util.UnitPrefix

/**
 * Utility for computing the major tick marks to use for a range of values.
 */
object Ticks {

  val defaultTimeFmt = DateTimeFormatter.ofPattern("MMMdd")

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
    minutes(1)   -> seconds(10),
    minutes(2)   -> seconds(30),
    minutes(3)   -> minutes(1),
    minutes(5)   -> minutes(1),
    minutes(10)  -> minutes(2),
    minutes(15)  -> minutes(5),
    minutes(20)  -> minutes(5),
    minutes(30)  -> minutes(10),
    hours(1)     -> minutes(10),
    hours(2)     -> minutes(30),
    hours(3)     -> hours(1),
    hours(4)     -> hours(1),
    hours(6)     -> hours(2),
    hours(8)     -> hours(2),
    hours(12)    -> hours(3),
    days(1)      -> hours(4),
    days(2)      -> hours(8),
    days(7)      -> days(1),
    days(2 * 7)  -> days(2),
    days(4 * 7)  -> days(1 * 7)
  )

  // Major and minor tick sizes for value axis
  private val valueTickSizes = List(
    10   -> 2,
    15   -> 5,
    20   -> 5,
    25   -> 5,
    30   -> 10,
    40   -> 10,
    50   -> 10,
    100  -> 20
  )

  /** Round to multiple of `s`. */
  private def round(v: Double, s: Double): Double = s * math.floor(v / s)

  private def digits(v: Double): Double = {
    val prefix = UnitPrefix.decimal(v)
    math.pow(10, math.ceil(math.log10(v / prefix.factor)))
  }

  private def offsetForRange(range: Double, v: Double): Double = {
    val absV = math.abs(v)
    if (range < absV / digits(absV)) v else 0.0
  }

  def value(v1: Double, v2: Double, n: Int): List[ValueTick] = {
    val range = v2 - v1
    val offset = offsetForRange(range, v1)

    val r = if (range < 1e-12) 1.0 else range
    val p = math.log10(r).toInt

    val fs = List(math.pow(10.0, p), math.pow(10.0, p + 1))
    val candidates = for (f <- fs; v <- valueTickSizes) yield (f / 100.0, v._1, v._2)
    val (f, major, minor) = candidates.filter(v => r / (v._1 * v._2) <= n).head

    val base = round(v1, f * major)
    val end = v2 - base
    val ticks = List.newBuilder[ValueTick]
    var pos = 0
    while (pos * f <= end) {
      val v = base + pos * f
      if (v >= v1) ticks += ValueTick(v, offset, pos % major == 0)
      pos += minor
    }
    ticks.result()
  }

  def time(s: Long, e: Long, zone: ZoneId, n: Int): List[TimeTick] = {
    // To keep even placement of major grid lines the shift amount for the timezone is computed
    // based on the start. If there is a change such as DST during the interval, then labels
    // after the change may be on less significant boundaries.
    val shift = zone.getRules.getOffset(Instant.ofEpochMilli(s)).getTotalSeconds * 1000L
    val dur = e - s
    val (major, minor) = timeTickSizes.filter(t => dur / t._1 <= n).head
    val ticks = List.newBuilder[TimeTick]

    val zs = s + shift
    val ze = e + shift
    var pos = zs / major * major
    while (pos <= ze) {
      if (pos >= zs) ticks += TimeTick(pos - shift, zone, pos % major == 0L)
      pos += minor
    }
    ticks.result()
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
 */
case class ValueTick(v: Double, offset: Double, major: Boolean = true) {
  def label: String = UnitPrefix.format(v - offset)
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
 */
case class TimeTick(timestamp: Long, zone: ZoneId, major: Boolean = true) {

  private val datetime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zone)

  import Ticks._

  def label: String = {
    val fmt = timeBoundaries.find(f => datetime.get(f._1) != 0).fold(defaultTimeFmt)(_._2)
    fmt.format(datetime)
  }
}
