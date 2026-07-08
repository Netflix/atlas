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
package com.netflix.atlas.core.algorithm

class OnlineCumulativeMaxSuite extends BaseOnlineAlgorithmSuite {

  override def newInstance: OnlineAlgorithm = OnlineCumulativeMax(Double.NaN)

  test("init") {
    val algo = OnlineCumulativeMax(Double.NaN)
    assertEquals(algo.next(1.0), 1.0)
    assertEquals(algo.next(1.0), 1.0)
  }

  test("increase") {
    val algo = OnlineCumulativeMax(Double.NaN)
    assertEquals(algo.next(1.0), 1.0)
    assertEquals(algo.next(2.0), 2.0)
    assertEquals(algo.next(3.0), 3.0)
    assertEquals(algo.next(7.0), 7.0)
  }

  test("decrease keeps the running max") {
    val algo = OnlineCumulativeMax(Double.NaN)
    assertEquals(algo.next(10.0), 10.0)
    assertEquals(algo.next(9.0), 10.0)
    assertEquals(algo.next(8.0), 10.0)
    assertEquals(algo.next(4.0), 10.0)
  }

  test("mixed") {
    val algo = OnlineCumulativeMax(Double.NaN)
    assertEquals(algo.next(1.0), 1.0)
    assertEquals(algo.next(3.0), 3.0)
    assertEquals(algo.next(2.0), 3.0)
    assertEquals(algo.next(5.0), 5.0)
    assertEquals(algo.next(0.0), 5.0)
  }

  test("negative values") {
    val algo = OnlineCumulativeMax(Double.NaN)
    assertEquals(algo.next(-5.0), -5.0)
    assertEquals(algo.next(-9.0), -5.0)
    assertEquals(algo.next(-2.0), -2.0)
  }

  test("NaN values ignored") {
    val algo = OnlineCumulativeMax(Double.NaN)
    assertEquals(algo.next(1.0), 1.0)
    assertEquals(algo.next(2.0), 2.0)
    assertEquals(algo.next(Double.NaN), 2.0)
    assertEquals(algo.next(1.0), 2.0)
    assertEquals(algo.next(3.0), 3.0)
  }

  test("leading NaN") {
    val algo = OnlineCumulativeMax(Double.NaN)
    assert(algo.next(Double.NaN).isNaN)
    assertEquals(algo.next(4.0), 4.0)
    assertEquals(algo.next(2.0), 4.0)
  }

  test("infinity") {
    val algo = OnlineCumulativeMax(Double.NaN)
    assertEquals(algo.next(Double.NegativeInfinity), Double.NegativeInfinity)
    assertEquals(algo.next(5.0), 5.0)
    assertEquals(algo.next(Double.PositiveInfinity), Double.PositiveInfinity)
    assertEquals(algo.next(1e300), Double.PositiveInfinity)
    assertEquals(algo.next(Double.NaN), Double.PositiveInfinity)
  }

  test("reset") {
    val algo = OnlineCumulativeMax(Double.NaN)
    assertEquals(algo.next(5.0), 5.0)
    assertEquals(algo.next(9.0), 9.0)
    algo.reset()
    assertEquals(algo.next(3.0), 3.0)
    assertEquals(algo.next(1.0), 3.0)
  }
}
