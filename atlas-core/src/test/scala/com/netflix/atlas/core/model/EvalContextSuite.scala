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
}
