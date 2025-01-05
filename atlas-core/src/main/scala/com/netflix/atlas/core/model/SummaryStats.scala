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

object SummaryStats {

  def apply(ts: TimeSeries, start: Long, end: Long): SummaryStats = apply(ts.data, start, end)

  def apply(ts: TimeSeq, start: Long, end: Long): SummaryStats = {
    var total = 0.0
    var count = 0
    var max = Double.NegativeInfinity
    var min = Double.PositiveInfinity
    var last = Double.NaN

    ts.foreach(start, end) { (_, v) =>
      if (!v.isNaN) {
        total += v
        count += 1
        max = if (v > max) v else max
        min = if (v < min) v else min
        last = v
      }
    }

    if (count == 0) SummaryStats.empty else SummaryStats(count, min, max, last, total)
  }

  val empty: SummaryStats = SummaryStats(0, Double.NaN, Double.NaN, Double.NaN, Double.NaN)
}

case class SummaryStats(count: Int, min: Double, max: Double, last: Double, total: Double) {

  def avg: Double = if (count > 0) total / count else Double.NaN

  /**
    * Tags corresponding to the stats available from `:stat` operator.
    */
  def tags(formatter: Double => String): Map[String, String] = Map(
    TagKey.avg   -> formatter(avg),
    TagKey.max   -> formatter(max),
    TagKey.min   -> formatter(min),
    TagKey.last  -> formatter(last),
    TagKey.total -> formatter(total)
  )

  /** Return the value of a statistic based on the name. */
  def get(stat: String): Double = stat match {
    case "avg"   => avg
    case "max"   => max
    case "min"   => min
    case "last"  => last
    case "total" => total
    case "count" => count
    case s       => throw new IllegalArgumentException(s"unknown statistic: $s")
  }
}
