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
package com.netflix.atlas.core.model

import java.time.Instant
import java.time.temporal.ChronoUnit

import munit.FunSuite

class EvalContextSuite extends FunSuite {

  // Was throwing:
  // requirement failed: start time must be less than end time (1495052210000 >= 1495051800000)
  test("partition by millis") {
    val s = Instant.parse("2017-05-17T20:16:50Z").toEpochMilli
    val e = Instant.parse("2017-05-17T20:46:50Z").toEpochMilli
    val step = 10000L
    val size = 60 * step
    val partitions = EvalContext(s, e, step).partition(size, ChronoUnit.MILLIS)
    assertEquals(partitions.size, 3)
  }

  private def millis(s: String): Long = Instant.parse(s).toEpochMilli

  test("partition iterator") {
    val step = 10000L
    val ctxt = EvalContext(millis("2017-05-17T20:16:50Z"), millis("2017-05-17T20:46:50Z"), step)
    val expected = List(
      EvalContext(millis("2017-05-17T20:16:50Z"), millis("2017-05-17T20:26:50Z"), step),
      EvalContext(millis("2017-05-17T20:26:50Z"), millis("2017-05-17T20:36:50Z"), step),
      EvalContext(millis("2017-05-17T20:36:50Z"), millis("2017-05-17T20:46:50Z"), step)
    )
    assertEquals(ctxt.partitionIterator(60 * step, ChronoUnit.MILLIS).toList, expected)
  }

  test("partition iterator, truncated start and end are clamped") {
    val step = 10000L
    val ctxt = EvalContext(millis("2017-05-17T20:16:50Z"), millis("2017-05-17T20:46:50Z"), step)
    val expected = List(
      EvalContext(millis("2017-05-17T20:16:50Z"), millis("2017-05-17T20:26:00Z"), step),
      EvalContext(millis("2017-05-17T20:26:00Z"), millis("2017-05-17T20:36:00Z"), step),
      EvalContext(millis("2017-05-17T20:36:00Z"), millis("2017-05-17T20:46:00Z"), step),
      EvalContext(millis("2017-05-17T20:46:00Z"), millis("2017-05-17T20:46:50Z"), step)
    )
    assertEquals(ctxt.partitionIterator(60 * step, ChronoUnit.MINUTES).toList, expected)
  }

  test("partition iterator, unit larger than partition step") {
    val step = 60000L
    val ctxt = EvalContext(millis("2017-05-17T20:30:00Z"), millis("2017-05-17T20:32:00Z"), step)
    val expected = List(
      EvalContext(millis("2017-05-17T20:30:00Z"), millis("2017-05-17T20:31:00Z"), step),
      EvalContext(millis("2017-05-17T20:31:00Z"), millis("2017-05-17T20:32:00Z"), step)
    )
    assertEquals(ctxt.partitionIterator(step, ChronoUnit.HOURS).toList, expected)
  }

  test("partition iterator, step must be positive") {
    val ctxt = EvalContext(0L, 60000L, 60000L)
    intercept[IllegalArgumentException](ctxt.partitionIterator(0L, ChronoUnit.MILLIS))
  }

  test("partition iterator is lazy") {
    // Range is large enough that eagerly materializing the partitions would be very slow
    val ctxt = EvalContext(0L, Long.MaxValue / 2, 1L)
    val first = ctxt.partitionIterator(1L, ChronoUnit.MILLIS).take(2).toList
    assertEquals(first, List(EvalContext(0L, 1L, 1L), EvalContext(1L, 2L, 1L)))
  }
}
