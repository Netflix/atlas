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
import java.time.temporal.ChronoUnit

import com.netflix.atlas.core.util.IdentityMap

case class EvalContext(
  start: Long,
  end: Long,
  step: Long,
  state: Map[StatefulExpr, Any] = IdentityMap.empty
) {

  require(start < end, s"start time must be less than end time ($start >= $end)")

  val noData: TimeSeries = TimeSeries.noData(step)

  /**
    * Buffer size that would be need to represent the result set based on the start time,
    * end time, and step size.
    */
  def bufferSize: Int = ((end - start) / step).toInt + 1

  def partition(oneStep: Long, unit: ChronoUnit): List[EvalContext] = {
    val builder = List.newBuilder[EvalContext]
    var t = Instant.ofEpochMilli(start).truncatedTo(unit).toEpochMilli
    while (t < end) {
      val e = t + oneStep
      val stime = math.max(t, start)
      val etime = math.min(e, end)
      builder += EvalContext(stime, etime, step)
      t = e
    }
    builder.result()
  }

  def partitionByHour: List[EvalContext] =
    partition(Duration.ofHours(1).toMillis, ChronoUnit.HOURS)

  def partitionByDay: List[EvalContext] = partition(Duration.ofDays(1).toMillis, ChronoUnit.DAYS)

  def withOffset(offset: Long): EvalContext = {
    val dur = offset / step * step
    if (dur < step) this else EvalContext(start - dur, end - dur, step, state)
  }

  def increaseStep(newStep: Long): EvalContext = {
    if (newStep == step)
      return this

    // Compute the new start / end time by rounding to step boundaries. The eval context
    // will end time is exclusive, so update by a step interval if the end time has changes
    // due to rounding.
    val s = start / newStep * newStep
    val e = end / newStep * newStep
    if (end == e)
      EvalContext(s, e, newStep)
    else
      EvalContext(s, e + newStep, newStep)
  }
}
