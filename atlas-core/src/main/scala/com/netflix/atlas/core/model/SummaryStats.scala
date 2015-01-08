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

object SummaryStats {

  import java.lang.{Double => JDouble}

  def apply(ts: TimeSeries, start: Long, end: Long): SummaryStats = {
    var total = 0.0
    var count = 0
    var max = JDouble.MIN_VALUE
    var min = JDouble.MAX_VALUE
    var last = JDouble.NaN

    ts.data.foreach(start, end) { (t, v) =>
      if (!JDouble.isNaN(v)) {
        total += v
        count += 1
        max = if (v > max) v else max
        min = if (v < min) v else min
        last = v
      }
    }

    if (count == 0) SummaryStats.empty else SummaryStats(count, min, max, last, total)
  }

  val empty = SummaryStats(0, Double.NaN, Double.NaN, Double.NaN, Double.NaN)
}

case class SummaryStats(count: Int, min: Double, max: Double, last: Double, total: Double) {
  def avg: Double = if (count > 0) total / count else Double.NaN
}
