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

class OnlineRollingMeanSuite extends BaseOnlineAlgorithmSuite {

  override def newInstance: OnlineAlgorithm = OnlineRollingMean(50, 30)

  test("n = 1, min = 1") {
    val algo = OnlineRollingMean(1, 1)
    assertEquals(algo.next(0.0), 0.0)
    assertEquals(algo.next(1.0), 1.0)
  }

  test("n = 2, min = 1") {
    val algo = OnlineRollingMean(2, 1)
    assertEquals(algo.next(0.0), 0.0)
    assertEquals(algo.next(1.0), 0.5)
    assertEquals(algo.next(2.0), 1.5)
  }

  test("n = 2, min = 2") {
    val algo = OnlineRollingMean(2, 2)
    assert(algo.next(0.0).isNaN)
    assertEquals(algo.next(1.0), 0.5)
    assertEquals(algo.next(2.0), 1.5)
  }

  test("n = 2, min = 1, decreasing") {
    val algo = OnlineRollingMean(2, 1)
    assertEquals(algo.next(2.0), 2.0)
    assertEquals(algo.next(1.0), 1.5)
    assertEquals(algo.next(0.0), 0.5)
  }

  test("n = 2, min = 1, NaNs") {
    val algo = OnlineRollingMean(2, 1)
    assertEquals(algo.next(0.0), 0.0)
    assertEquals(algo.next(1.0), 0.5)
    assertEquals(algo.next(2.0), 1.5)
    assertEquals(algo.next(Double.NaN), 2.0)
    assert(!algo.isEmpty)
    assert(algo.next(Double.NaN).isNaN)
    assert(algo.isEmpty)
  }

  test("n = 2, recover from large value") {
    val algo = OnlineRollingMean(2, 1)
    assertEquals(algo.next(0.0), 0.0)
    assertEquals(algo.next(1.0e300), 5.0e299)
    assertEquals(algo.next(2.0), 5.0e299)
    assertEquals(algo.next(3.0), 2.5)
  }

  test("n = 2, recover from small value") {
    val algo = OnlineRollingMean(2, 1)
    assertEquals(algo.next(0.0), 0.0)
    assertEquals(algo.next(-1.0e300), -5.0e299)
    assertEquals(algo.next(2.0), -5.0e299)
    assertEquals(algo.next(3.0), 2.5)
  }

  test("n = 2, min = 1, reset") {
    val algo = OnlineRollingMean(2, 1)
    assertEquals(algo.next(1.0), 1.0)
    algo.reset()
    assertEquals(algo.next(5.0), 5.0)
  }

  test("min < n") {
    intercept[IllegalArgumentException] {
      OnlineRollingMean(1, 2)
    }
  }

  test("min = 0") {
    intercept[IllegalArgumentException] {
      OnlineRollingMean(2, 0)
    }
  }

  test("min < 0") {
    intercept[IllegalArgumentException] {
      OnlineRollingMean(2, -4)
    }
  }
}
