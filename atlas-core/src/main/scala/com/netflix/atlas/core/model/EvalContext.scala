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

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

case class EvalContext(
    start: Long,
    end: Long,
    step: Long,
    state: Map[StatefulExpr, Any] = Map.empty) {
  require(start < end, "start time must be less than end time")

  val noData: TimeSeries = TimeSeries.noData(step)

  def partition(oneStep: Long, unit: ChronoUnit): List[EvalContext] = {
    val builder = List.newBuilder[EvalContext]
    var t = Instant.ofEpochMilli(start).truncatedTo(ChronoUnit.HOURS).toEpochMilli
    while (t < end) {
      val e = t + oneStep
      val stime = if (t >= start) t else start
      val etime = if (e >= end) end else e
      builder += EvalContext(stime, etime, step)
      t = e
    }
    builder.result
  }

  def partitionByHour: List[EvalContext] = partition(Duration.ofHours(1).toMillis, ChronoUnit.HOURS)
  def partitionByDay: List[EvalContext] = partition(Duration.ofDays(1).toMillis, ChronoUnit.DAYS)

  def withOffset(offset: Long): EvalContext = {
    val dur = offset / step * step
    if (dur < step) this else EvalContext(start - dur, end - dur, step, state)
  }
}
