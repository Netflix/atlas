/*
 * Copyright 2014-2026 Netflix, Inc.
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

  private def minutes(n: Long): Long = n * 60 * 1000

  private def days(n: Long): Long = n * 24 * 60 * 60 * 1000

  private val oneHourStep = Step.forBlockStep(minutes(60))

  test("round: sub-second step sizes") {
    val primaryStep = 1L
    assertEquals(1L, oneHourStep.round(primaryStep, 1))
    assertEquals(5L, oneHourStep.round(primaryStep, 2))
    assertEquals(10L, oneHourStep.round(primaryStep, 6))
    assertEquals(50L, oneHourStep.round(primaryStep, 20))
    assertEquals(100L, oneHourStep.round(primaryStep, 60))
    assertEquals(500L, oneHourStep.round(primaryStep, 200))
    assertEquals(1000L, oneHourStep.round(primaryStep, 600))
  }

  test("round: allow arbitrary number of days") {
    (1 until 500).foreach { i =>
      assertEquals(days(i), oneHourStep.round(60000, days(i)))
    }
  }

  test("round: up if less than a day") {
    assertEquals(days(1), oneHourStep.round(60000, days(1) / 2 + 1))
  }

  test("round: up if not on day boundary") {
    assertEquals(days(3), oneHourStep.round(60000, days(2) + 1))
  }

  test("round: only use steps that evenly divide block") {
    val primaryStep = 5_000L
    val tenMinuteStep = Step.forBlockStep(minutes(10))
    assertEquals(minutes(1), tenMinuteStep.round(primaryStep, minutes(1)))
    assertEquals(minutes(2), tenMinuteStep.round(primaryStep, minutes(2)))
    assertEquals(minutes(5), tenMinuteStep.round(primaryStep, minutes(3)))
    assertEquals(minutes(5), tenMinuteStep.round(primaryStep, minutes(4)))
    assertEquals(minutes(5), tenMinuteStep.round(primaryStep, minutes(5)))
    assertEquals(minutes(10), tenMinuteStep.round(primaryStep, minutes(6)))
  }

  test("round: only use steps that are multiples of the block") {
    val primaryStep = 5_000L
    val tenMinuteStep = Step.forBlockStep(minutes(10))
    assertEquals(minutes(20), tenMinuteStep.round(primaryStep, minutes(15)))
  }

  test("compute: allow arbitrary number of days") {
    (1 until 500).foreach { i =>
      assertEquals(days(i), oneHourStep.compute(60000, 1, 0L, days(i)))
    }
  }

  test("compute: round less than a day") {
    val e = Instant.parse("2017-06-27T00:00:00Z")
    val s = e.minus(12 * 30, ChronoUnit.DAYS)
    assertEquals(days(1), oneHourStep.compute(60000, 430, s.toEpochMilli, e.toEpochMilli))
  }

  test("roundToStepBoundary: 2ms step") {
    assertEquals(Step.roundToStepBoundary(0L, 2L), 0L)
    assertEquals(Step.roundToStepBoundary(1L, 2L), 2L)
    assertEquals(Step.roundToStepBoundary(2L, 2L), 2L)
    assertEquals(Step.roundToStepBoundary(3L, 2L), 4L)
    assertEquals(Step.roundToStepBoundary(4L, 2L), 4L)
  }

  test("roundToStepBoundary: 5s step") {
    assertEquals(Step.roundToStepBoundary(0L, 5_000L), 0L)
    assertEquals(Step.roundToStepBoundary(1L, 5_000L), 5_000L)
    assertEquals(Step.roundToStepBoundary(2_000L, 5_000L), 5_000L)
    assertEquals(Step.roundToStepBoundary(5_000L, 5_000L), 5_000L)
    assertEquals(Step.roundToStepBoundary(11_000L, 5_000L), 15_000L)
  }

  test("roundToStepBoundary: 60s step") {
    assertEquals(Step.roundToStepBoundary(0L, 60_000L), 0L)
    assertEquals(Step.roundToStepBoundary(1L, 60_000L), 60_000L)
    assertEquals(Step.roundToStepBoundary(87_123L, 60_000L), 120_000L)
  }

  test("firstPrimaryStepTimestamp: multiple = 1") {
    // No consolidation, timestamp should be returned as-is
    assertEquals(Step.firstPrimaryStepTimestamp(60_000L, 60_000L, 1), 60_000L)
    assertEquals(Step.firstPrimaryStepTimestamp(0L, 60_000L, 1), 0L)
    assertEquals(Step.firstPrimaryStepTimestamp(180_000L, 60_000L, 1), 180_000L)
  }

  test("firstPrimaryStepTimestamp: example from documentation") {
    // Consolidated interval timestamp = 1:03 (3 minutes = 180_000ms)
    // Step = 3 minutes, Multiple = 3
    // Primary step = 1 minute
    // Result should be 1:01 (60_000ms)
    val timestamp = minutes(3)
    val step = minutes(3)
    val multiple = 3
    val expected = minutes(1)
    assertEquals(Step.firstPrimaryStepTimestamp(timestamp, step, multiple), expected)
  }

  test("firstPrimaryStepTimestamp: 1 hour with multiple = 60") {
    // Consolidated interval timestamp = 1 hour
    // Step = 1 hour, Multiple = 60
    // Primary step = 1 minute
    // Result should be 1 minute
    val timestamp = minutes(60)
    val step = minutes(60)
    val multiple = 60
    val expected = minutes(1)
    assertEquals(Step.firstPrimaryStepTimestamp(timestamp, step, multiple), expected)
  }

  test("firstPrimaryStepTimestamp: 10 minutes with multiple = 10") {
    // Consolidated interval timestamp = 10 minutes
    // Step = 10 minutes, Multiple = 10
    // Primary step = 1 minute
    // Result should be 1 minute
    val timestamp = minutes(10)
    val step = minutes(10)
    val multiple = 10
    val expected = minutes(1)
    assertEquals(Step.firstPrimaryStepTimestamp(timestamp, step, multiple), expected)
  }

  test("firstPrimaryStepTimestamp: zero timestamp") {
    // At timestamp = 0, the first primary step timestamp should be negative
    // For step = 3m, multiple = 3, primary step = 1m
    // Result: 0 - 180_000 + 60_000 = -120_000
    val timestamp = 0L
    val step = minutes(3)
    val multiple = 3
    val expected = -minutes(2)
    assertEquals(Step.firstPrimaryStepTimestamp(timestamp, step, multiple), expected)
  }

  test("firstPrimaryStepTimestamp: multiple = 2") {
    // Consolidated interval timestamp = 2 minutes
    // Step = 2 minutes, Multiple = 2
    // Primary step = 1 minute
    // Result should be 1 minute
    val timestamp = minutes(2)
    val step = minutes(2)
    val multiple = 2
    val expected = minutes(1)
    assertEquals(Step.firstPrimaryStepTimestamp(timestamp, step, multiple), expected)
  }

  test("firstPrimaryStepTimestamp: large timestamp") {
    // Test with a large timestamp to ensure calculation handles overflow correctly
    val timestamp = minutes(1000)
    val step = minutes(10)
    val multiple = 10
    val expected = minutes(1000) - minutes(10) + minutes(1)
    assertEquals(Step.firstPrimaryStepTimestamp(timestamp, step, multiple), expected)
  }

  test("firstPrimaryStepTimestamp: timestamp not on boundary") {
    // Should fail with require check
    intercept[IllegalArgumentException] {
      Step.firstPrimaryStepTimestamp(61_000L, 60_000L, 1)
    }
  }

  test("firstPrimaryStepTimestamp: step not divisible by multiple") {
    // Should fail with require check
    intercept[IllegalArgumentException] {
      Step.firstPrimaryStepTimestamp(60_000L, 60_000L, 7)
    }
  }
}
