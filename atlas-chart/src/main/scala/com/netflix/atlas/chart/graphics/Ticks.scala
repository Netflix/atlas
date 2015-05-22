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

  private val timeTickSizes = List(
    Duration.ofMinutes(1).toMillis,
    Duration.ofMinutes(2).toMillis,
    Duration.ofMinutes(3).toMillis,
    Duration.ofMinutes(5).toMillis,
    Duration.ofMinutes(10).toMillis,
    Duration.ofMinutes(15).toMillis,
    Duration.ofMinutes(20).toMillis,
    Duration.ofMinutes(30).toMillis,
    Duration.ofHours(1).toMillis,
    Duration.ofHours(2).toMillis,
    Duration.ofHours(3).toMillis,
    Duration.ofHours(4).toMillis,
    Duration.ofHours(6).toMillis,
    Duration.ofHours(8).toMillis,
    Duration.ofHours(12).toMillis,
    Duration.ofDays(1).toMillis,
    Duration.ofDays(2).toMillis,
    Duration.ofDays(7).toMillis,
    Duration.ofDays(2 * 7).toMillis,
    Duration.ofDays(4 * 7).toMillis,
    Duration.ofDays(4 * 7).toMillis
  )

  private val valueTickSizes = List(
    1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 10.0
  )

  /** Round to multiple of `s`. */
  private def round(v: Double, s: Double): Double = s * math.floor(v / s)

  def digits(v: Double): Double = {
    val prefix = UnitPrefix.decimal(v)
    math.pow(10, math.ceil(math.log10(v / prefix.factor)))
  }

  def value(v1: Double, v2: Double, n: Int): List[ValueTick] = {
    val range = v2 - v1
    val base = math.abs(v1)
    val offset = if (range < base / digits(base)) v1 else 0.0

    val r = if (range < 1e-12) 1.0 else range
    val p = math.log10(r).toInt - 1
    val fs = List(math.pow(10.0, p), math.pow(10.0, p + 1))
    val candidates = for (f <- fs; v <- valueTickSizes) yield v * f
    val size = candidates.filter(v => r / v <= n).head
    val ticks = List.newBuilder[ValueTick]
    var pos = round(v1, size)
    while (pos <= v2) {
      if (pos >= v1) ticks += ValueTick(pos, offset)
      pos += size
    }
    ticks.result()
  }

  def time(s: Long, e: Long, zone: ZoneId, n: Int): List[TimeTick] = {
    val dur = e - s
    val size = timeTickSizes.filter(t => dur / t <= n).head
    val ticks = List.newBuilder[TimeTick]
    var pos = if (s % size == 0) s else (s / size * size + size)
    while (pos <= e) {
      ticks += TimeTick(pos, zone)
      pos += size
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
 */
case class ValueTick(v: Double, offset: Double) {
  def label: String = UnitPrefix.format(v - offset)
}

/**
 * Tick mark for the time axis.
 *
 * @param timestamp
 *     Time in milliseconds since the epoch.
 * @param zone
 *     Time zone to use for the string label associated with the timestamp.
 */
case class TimeTick(timestamp: Long, zone: ZoneId) {

  private val datetime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zone)

  import Ticks._

  def label: String = {
    val fmt = timeBoundaries.find(f => datetime.get(f._1) != 0).fold(defaultTimeFmt)(_._2)
    fmt.format(datetime)
  }
}
