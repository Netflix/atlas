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

import java.time.Instant
import java.time.temporal.ChronoUnit

import munit.FunSuite

class StepSuite extends FunSuite {

  private def days(n: Long): Long = n * 24 * 60 * 60 * 1000

  test("round: sub-second step sizes") {
    val primaryStep = 1L
    assertEquals(1L, Step.round(primaryStep, 1))
    assertEquals(5L, Step.round(primaryStep, 2))
    assertEquals(10L, Step.round(primaryStep, 6))
    assertEquals(50L, Step.round(primaryStep, 20))
    assertEquals(100L, Step.round(primaryStep, 60))
    assertEquals(500L, Step.round(primaryStep, 200))
    assertEquals(1000L, Step.round(primaryStep, 600))
  }

  test("round: allow arbitrary number of days") {
    (1 until 500).foreach { i =>
      assertEquals(days(i), Step.round(60000, days(i)))
    }
  }

  test("round: up if less than a day") {
    assertEquals(days(1), Step.round(60000, days(1) / 2 + 1))
  }

  test("round: up if not on day boundary") {
    assertEquals(days(3), Step.round(60000, days(2) + 1))
  }

  test("compute: allow arbitrary number of days") {
    (1 until 500).foreach { i =>
      assertEquals(days(i), Step.compute(60000, 1, 0L, days(i)))
    }
  }

  test("compute: round less than a day") {
    val e = Instant.parse("2017-06-27T00:00:00Z")
    val s = e.minus(12 * 30, ChronoUnit.DAYS)
    assertEquals(days(1), Step.compute(60000, 430, s.toEpochMilli, e.toEpochMilli))
  }
}
