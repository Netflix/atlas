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

class OnlineTrendSuite extends BaseOnlineAlgorithmSuite {

  override def newInstance: OnlineAlgorithm = OnlineTrend(50)

  test("n = 1") {
    val algo = OnlineTrend(1)
    assertEquals(algo.next(0.0), 0.0)
    assertEquals(algo.next(1.0), 1.0)
  }

  test("n = 2") {
    val algo = OnlineTrend(2)
    assert(algo.next(0.0).isNaN)
    assertEquals(algo.next(1.0), 0.5)
    assertEquals(algo.next(2.0), 1.5)
  }

  test("n = 2, decreasing") {
    val algo = OnlineTrend(2)
    assert(algo.next(2.0).isNaN)
    assertEquals(algo.next(1.0), 1.5)
    assertEquals(algo.next(0.0), 0.5)
  }

  test("n = 2, NaNs") {
    val algo = OnlineTrend(2)
    assert(algo.next(0.0).isNaN)
    assertEquals(algo.next(1.0), 0.5)
    assertEquals(algo.next(2.0), 1.5)
    assertEquals(algo.next(Double.NaN), 1.0)
    assert(!algo.isEmpty)
    assert(algo.next(Double.NaN).isNaN)
    assert(algo.isEmpty)
  }

  test("n = 2, reset") {
    val algo = OnlineTrend(2)
    assert(algo.next(1.0).isNaN)
    algo.reset()
    assert(algo.next(1.0).isNaN)
  }
}
