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
package com.netflix.atlas.core.util

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
  * Interval that moves over time in increments of a given unit. When in the middle of
  * a unit it will round to the next even boundary. For example, if the unit is HOURS
  * and it is 10:37, then it will round too 11:00.
  *
  * The offset and duration must be an even multiple of the unit.
  *
  * @param offset
  *     Offset subtracted from the current time, `now - offset` is used as the end
  *     time for this interval.
  * @param duration
  *     The length of the interval. The start time is `now - offset - duration`.
  * @param unit
  *     The unit to use when moving along. This is typically HOURS or DAYS.
  */
case class RollingInterval(offset: Duration, duration: Duration, unit: ChronoUnit) {

  import RollingInterval.*

  checkParams(offset, duration, unit)

  /** Current end time for this interval. */
  def end: Instant = ceil(Instant.now(), unit).minus(offset)

  /** Current start time for this interval. */
  def start: Instant = end.minus(duration)

  private def currentRange: (Long, Long) = {
    val e = end
    val s = e.minus(duration)
    s.toEpochMilli -> e.toEpochMilli
  }

  /** Returns true if the instant is currently within this interval. */
  def contains(instant: Instant): Boolean = {
    val t = instant.toEpochMilli
    val (s, e) = currentRange
    s <= t && t <= e
  }

  private def isContainedBy(s: Instant, e: Instant): Boolean = {
    val s1 = s.toEpochMilli
    val e1 = e.toEpochMilli
    val (s2, e2) = currentRange
    s1 < s2 && e1 > e2
  }

  /** Returns true if the interval [s, e] overlaps with this interval. */
  def overlaps(s: Instant, e: Instant): Boolean = {
    contains(s) || contains(e) || isContainedBy(s, e)
  }
}

object RollingInterval {

  /**
    * Create a new interval from a string representation. The string value should have the
    * format: `offset,duration,unit`. Offset and duration will be parsed by using the
    * Strings.parseDuration helper. Unit should be `HOURS` or `DAYS`.
    */
  def apply(interval: String): RollingInterval = {
    val parts = interval.split("\\s*,\\s*")
    require(parts.length == 3, s"invalid rolling interval: $interval")
    val s = Strings.parseDuration(parts(0))
    val e = Strings.parseDuration(parts(1))
    val unit = ChronoUnit.valueOf(parts(2))
    RollingInterval(s, e, unit)
  }

  private def checkParams(offset: Duration, duration: Duration, unit: ChronoUnit): Unit = {
    val unitDuration = Duration.of(1, unit)
    require(duration.toMillis >= unitDuration.toMillis, s"duration $duration <= $unitDuration")

    require(offset.toMillis % unitDuration.toMillis == 0, s"offset must be multiple of $unit")
    require(duration.toMillis % unitDuration.toMillis == 0, s"duration must be multiple of $unit")
  }

  private[util] def ceil(t: Instant, unit: ChronoUnit): Instant = {
    val truncated = t.truncatedTo(unit)
    if (truncated == t) t else truncated.plus(1, unit)
  }
}
