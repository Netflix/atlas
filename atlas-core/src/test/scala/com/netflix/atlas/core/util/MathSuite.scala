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

import munit.FunSuite

class MathSuite extends FunSuite {

  import java.lang.Double as JDouble

  import com.netflix.atlas.core.util.Math.*

  test("isNearlyZero") {
    assertEquals(isNearlyZero(1.0), false)
    assertEquals(isNearlyZero(-1000.0), false)
    assertEquals(isNearlyZero(0.0), true)
    assertEquals(isNearlyZero(1e-12), false)
    assertEquals(isNearlyZero(1e-13), true)
    assertEquals(isNearlyZero(Double.NaN), true)
  }

  test("toBoolean") {
    assertEquals(toBoolean(1.0), true)
    assertEquals(toBoolean(-1000.0), true)
    assertEquals(toBoolean(0.0), false)
    assertEquals(toBoolean(1e-12), true)
    assertEquals(toBoolean(1e-13), false)
    assertEquals(toBoolean(Double.NaN), false)
  }

  test("addNaN") {
    assertEquals(addNaN(1.0, 2.0), 3.0)
    assertEquals(addNaN(Double.NaN, 2.0), 2.0)
    assertEquals(addNaN(1.0, Double.NaN), 1.0)
    assert(JDouble.isNaN(addNaN(Double.NaN, Double.NaN)))
  }

  test("subtractNaN") {
    assertEquals(subtractNaN(1.0, 2.0), -1.0)
    assertEquals(subtractNaN(Double.NaN, 2.0), -2.0)
    assertEquals(subtractNaN(1.0, Double.NaN), 1.0)
    assert(JDouble.isNaN(subtractNaN(Double.NaN, Double.NaN)))
  }

  test("maxNaN") {
    assertEquals(maxNaN(1.0, 2.0), 2.0)
    assertEquals(maxNaN(2.0, 1.0), 2.0)
    assertEquals(maxNaN(Double.NaN, 2.0), 2.0)
    assertEquals(maxNaN(1.0, Double.NaN), 1.0)
    assert(JDouble.isNaN(maxNaN(Double.NaN, Double.NaN)))
  }

  test("minNaN") {
    assertEquals(minNaN(1.0, 2.0), 1.0)
    assertEquals(minNaN(2.0, 1.0), 1.0)
    assertEquals(minNaN(Double.NaN, 2.0), 2.0)
    assertEquals(minNaN(1.0, Double.NaN), 1.0)
    assert(JDouble.isNaN(minNaN(Double.NaN, Double.NaN)))
  }

  test("gtNaN") {
    assertEquals(gtNaN(1.0, 2.0), 0.0)
    assertEquals(gtNaN(2.0, 1.0), 1.0)
    assertEquals(gtNaN(Double.NaN, 2.0), 0.0)
    assertEquals(gtNaN(1.0, Double.NaN), 1.0)
    assert(JDouble.isNaN(gtNaN(Double.NaN, Double.NaN)))
  }

  test("ltNaN") {
    assertEquals(ltNaN(1.0, 2.0), 1.0)
    assertEquals(ltNaN(2.0, 1.0), 0.0)
    assertEquals(ltNaN(Double.NaN, 2.0), 1.0)
    assertEquals(ltNaN(1.0, Double.NaN), 0.0)
    assert(JDouble.isNaN(ltNaN(Double.NaN, Double.NaN)))
  }

}
