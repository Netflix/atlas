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
package com.netflix.atlas.core.util

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

import munit.FunSuite

class RollingIntervalSuite extends FunSuite {

  test("ceil should not round up if on exact boundary") {
    val expected = Instant.parse("2007-12-03T10:00:00.00Z")
    val t = Instant.parse("2007-12-03T10:00:00.00Z")
    assertEquals(RollingInterval.ceil(t, ChronoUnit.HOURS), expected)
  }

  test("ceil should round up to next boundary MINUTES") {
    val expected = Instant.parse("2007-12-03T10:16:00.00Z")
    val t = Instant.parse("2007-12-03T10:15:30.00Z")
    assertEquals(RollingInterval.ceil(t, ChronoUnit.MINUTES), expected)
  }

  test("ceil should round up to next boundary HOURS") {
    val expected = Instant.parse("2007-12-03T11:00:00.00Z")
    val t = Instant.parse("2007-12-03T10:15:30.00Z")
    assertEquals(RollingInterval.ceil(t, ChronoUnit.HOURS), expected)
  }

  test("ceil should round up to next boundary DAYS") {
    val expected = Instant.parse("2007-12-04T00:00:00.00Z")
    val t = Instant.parse("2007-12-03T10:15:30.00Z")
    assertEquals(RollingInterval.ceil(t, ChronoUnit.DAYS), expected)
  }

  test("from string") {
    val expected = RollingInterval(Duration.ZERO, Duration.ofDays(4), ChronoUnit.HOURS)
    val actual = RollingInterval("0h,4d,HOURS")
    assertEquals(actual, expected)
  }

  test("from string, missing unit") {
    val e = intercept[IllegalArgumentException] {
      RollingInterval("0h,4d")
    }
    assert(e.getMessage.contains("invalid rolling interval: 0h,4d"))
  }

  test("from string, invalid unit") {
    val e = intercept[IllegalArgumentException] {
      RollingInterval("0h,4d,foo")
    }
    assert(e.getMessage.contains("No enum constant"))
  }

  test("from string, invalid duration") {
    val e = intercept[IllegalArgumentException] {
      RollingInterval("foo,4d,HOURS")
    }
    assert(e.getMessage.contains("invalid period foo"))
  }

  test("duration is less than 1 unit") {
    val e = intercept[IllegalArgumentException] {
      RollingInterval(Duration.ZERO, Duration.ZERO, ChronoUnit.DAYS)
    }
    assert(e.getMessage.contains("duration PT0S <= PT24H"))
  }

  test("offset is not multiple of unit") {
    val e = intercept[IllegalArgumentException] {
      RollingInterval(Duration.ofHours(3), Duration.ofDays(4), ChronoUnit.DAYS)
    }
    assert(e.getMessage.contains("offset must be multiple of Days"))
  }

  test("duration is not multiple of unit") {
    val e = intercept[IllegalArgumentException] {
      RollingInterval(Duration.ZERO, Duration.ofHours(47), ChronoUnit.DAYS)
    }
    assert(e.getMessage.contains("duration must be multiple of Days"))
  }

  test("contains start") {
    val interval = RollingInterval(Duration.ofDays(3), Duration.ofDays(1), ChronoUnit.DAYS)
    assert(interval.contains(interval.start))
  }

  test("contains end") {
    val interval = RollingInterval(Duration.ofDays(3), Duration.ofDays(1), ChronoUnit.DAYS)
    assert(interval.contains(interval.end))
  }

  test("contains times between start and end") {
    val interval = RollingInterval(Duration.ofDays(3), Duration.ofDays(1), ChronoUnit.DAYS)
    assert(interval.contains(interval.start.plusMillis(1)))
  }

  test("does not contain times before start") {
    val interval = RollingInterval(Duration.ofDays(3), Duration.ofDays(1), ChronoUnit.DAYS)
    assert(!interval.contains(interval.start.minusMillis(1)))
  }

  test("does not contain times after end") {
    val interval = RollingInterval(Duration.ofDays(3), Duration.ofDays(1), ChronoUnit.DAYS)
    assert(!interval.contains(interval.end.plusMillis(1)))
  }

  test("overlaps start") {
    val s = Instant.now().minus(4, ChronoUnit.DAYS)
    val interval = RollingInterval(Duration.ofDays(3), Duration.ofDays(2), ChronoUnit.DAYS)
    assert(interval.overlaps(s, s.plus(2, ChronoUnit.DAYS)))
  }

  test("overlaps start exact") {
    val interval = RollingInterval(Duration.ofDays(3), Duration.ofDays(2), ChronoUnit.DAYS)
    val s = interval.start
    assert(interval.overlaps(s, s.plus(2, ChronoUnit.DAYS)))
  }

  test("overlaps end") {
    val e = Instant.now().minus(4, ChronoUnit.DAYS)
    val interval = RollingInterval(Duration.ofDays(3), Duration.ofDays(2), ChronoUnit.DAYS)
    assert(interval.overlaps(e.minus(2, ChronoUnit.DAYS), e))
  }

  test("overlaps end exact") {
    val interval = RollingInterval(Duration.ofDays(3), Duration.ofDays(2), ChronoUnit.DAYS)
    val e = interval.end
    assert(interval.overlaps(e.minus(2, ChronoUnit.DAYS), e))
  }

  test("overlaps start and end") {
    val s = Instant.now().minus(4, ChronoUnit.DAYS)
    val interval = RollingInterval(Duration.ofDays(3), Duration.ofDays(2), ChronoUnit.DAYS)
    assert(interval.overlaps(s, s.plus(2, ChronoUnit.HOURS)))
  }

  test("overlaps start and end exact") {
    val interval = RollingInterval(Duration.ofDays(3), Duration.ofDays(2), ChronoUnit.DAYS)
    assert(interval.overlaps(interval.start, interval.end))
  }

  test("overlaps when contained by interval") {
    val interval = RollingInterval(Duration.ofDays(3), Duration.ofDays(2), ChronoUnit.DAYS)
    assert(interval.overlaps(interval.start.minusSeconds(1), interval.end.plusSeconds(1)))
  }

  test("does not overlap when start is after end") {
    val interval = RollingInterval(Duration.ofDays(3), Duration.ofDays(2), ChronoUnit.DAYS)
    val e = interval.end
    assert(!interval.overlaps(e.plus(1, ChronoUnit.DAYS), e.plus(2, ChronoUnit.DAYS)))
  }

  test("does not overlap when end is before start") {
    val interval = RollingInterval(Duration.ofDays(3), Duration.ofDays(2), ChronoUnit.DAYS)
    val s = interval.start
    assert(!interval.overlaps(s.minus(2, ChronoUnit.DAYS), s.minus(1, ChronoUnit.DAYS)))
  }
}
