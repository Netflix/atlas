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
package com.netflix.atlas.core.algorithm

import munit.FunSuite

class RollingBufferSuite extends FunSuite {

  test("n = 0") {
    intercept[IllegalArgumentException] {
      RollingBuffer(0)
    }
  }

  test("n = 1, add") {
    val buf = RollingBuffer(1)
    assert(buf.add(0.0).isNaN)
    assertEquals(buf.add(1.0), 0.0)
    assertEquals(buf.add(2.0), 1.0)
  }

  test("n = 2, add") {
    val buf = RollingBuffer(2)
    assert(buf.add(0.0).isNaN)
    assert(buf.add(1.0).isNaN)
    assertEquals(buf.add(2.0), 0.0)
    assertEquals(buf.add(3.0), 1.0)
    assertEquals(buf.add(4.0), 2.0)
    assertEquals(buf.add(5.0), 3.0)
    assertEquals(buf.add(6.0), 4.0)
  }

  test("n = 2, add NaNs") {
    val buf = RollingBuffer(2)
    assert(buf.add(0.0).isNaN)
    assert(buf.add(1.0).isNaN)
    assertEquals(buf.add(2.0), 0.0)
    assertEquals(buf.add(3.0), 1.0)
    assertEquals(buf.add(Double.NaN), 2.0)
    assertEquals(buf.add(5.0), 3.0)
    assert(buf.add(6.0).isNaN)
  }

  test("n = 2, size") {
    val buf = RollingBuffer(2)
    assert(buf.size == 0)

    buf.add(0.0)
    assert(buf.size == 1)

    buf.add(1.0)
    assert(buf.size == 2)

    buf.add(2.0)
    assert(buf.size == 2)

    buf.add(3.0)
    assert(buf.size == 2)

    buf.add(Double.NaN)
    assert(buf.size == 1)

    buf.add(Double.NaN)
    assert(buf.size == 0)

    buf.add(Double.NaN)
    assert(buf.size == 0)
  }

  test("n = 2, min") {
    val buf = RollingBuffer(2)
    assert(buf.min.isNaN)

    buf.add(0.0)
    assert(buf.min == 0.0)

    buf.add(1.0)
    assert(buf.min == 0.0)

    buf.add(2.0)
    assert(buf.min == 1.0)

    buf.add(3.0)
    assert(buf.min == 2.0)

    buf.add(Double.NaN)
    assert(buf.min == 3.0)

    buf.add(Double.NaN)
    assert(buf.min.isNaN)
  }

  test("n = 2, max") {
    val buf = RollingBuffer(2)
    assert(buf.max.isNaN)

    buf.add(0.0)
    assert(buf.max == 0.0)

    buf.add(1.0)
    assert(buf.max == 1.0)

    buf.add(2.0)
    assert(buf.max == 2.0)

    buf.add(3.0)
    assert(buf.max == 3.0)

    buf.add(Double.NaN)
    assert(buf.max == 3.0)

    buf.add(Double.NaN)
    assert(buf.max.isNaN)
  }

  test("n = 2, clear") {
    val buf = RollingBuffer(2)
    buf.add(0.0)
    buf.add(1.0)
    buf.clear()
    assert(buf.size == 0)
  }

  test("n = 2, state") {
    val buf = RollingBuffer(2)
    buf.add(0.0)
    val cfg = buf.state
    assertEquals(cfg.getInt("pos"), 1)
    val actual = cfg.getDoubleArray("values")
    val expected = List(0.0, Double.NaN)
    assertEquals(actual.length, 2)
    actual.zip(expected).foreach {
      // Used Double.compare to handle NaN properly
      case (v1, v2) => assertEquals(java.lang.Double.compare(v1, v2), 0, s"$v1 != $v2")
    }
  }

  test("restore from state") {
    val buf = RollingBuffer(50)
    var restoredBuf = RollingBuffer(buf.state)
    val random = new java.util.Random()
    val n = random.nextInt(200) + 50

    (0 until 10000).foreach { i =>
      val v = if (random.nextDouble() < 0.01) Double.NaN else random.nextDouble()
      val expected = buf.add(v)
      val actual = restoredBuf.add(v)
      assertEquals(java.lang.Double.compare(actual, expected), 0, s"$actual != $expected")
      if (i % n == 0) {
        restoredBuf = RollingBuffer(buf.state)
      }
    }
  }
}
