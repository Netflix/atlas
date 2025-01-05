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
package com.netflix.atlas.postgres

import munit.FunSuite

class IntervalSuite extends FunSuite {

  test("overlaps: none") {
    val i1 = Interval(0, 100)
    val i2 = Interval(101, 102)
    assert(!i1.overlaps(i2))
  }

  test("overlaps: i2 starts before end of i1") {
    val i1 = Interval(0, 105)
    val i2 = Interval(101, 120)
    assert(i1.overlaps(i2))
  }

  test("overlaps: i1 starts before end of i2") {
    val i1 = Interval(101, 120)
    val i2 = Interval(0, 105)
    assert(i1.overlaps(i2))
  }

  test("overlaps: i2 within i1") {
    val i1 = Interval(0, 105)
    val i2 = Interval(101, 102)
    assert(i1.overlaps(i2))
  }

  test("overlaps: i1 within i2") {
    val i1 = Interval(101, 102)
    val i2 = Interval(10, 150)
    assert(i1.overlaps(i2))
  }
}
