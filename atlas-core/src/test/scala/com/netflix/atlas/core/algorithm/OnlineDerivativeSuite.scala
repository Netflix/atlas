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

class OnlineDerivativeSuite extends BaseOnlineAlgorithmSuite {

  override def newInstance: OnlineAlgorithm = OnlineDerivative(Double.NaN)

  test("init") {
    val algo = OnlineDerivative(Double.NaN)
    assert(algo.next(1.0).isNaN)
    assertEquals(algo.next(1.0), 0.0)
  }

  test("NaN values") {
    val algo = OnlineDerivative(Double.NaN)
    assert(algo.next(1.0).isNaN)
    assertEquals(algo.next(1.0), 0.0)
    assert(algo.next(Double.NaN).isNaN)
    assert(algo.next(1.0).isNaN)
    assertEquals(algo.next(1.0), 0.0)
  }

  test("increase") {
    val algo = OnlineDerivative(Double.NaN)
    assert(algo.next(1.0).isNaN)
    assertEquals(algo.next(2.0), 1.0)
    assertEquals(algo.next(3.0), 1.0)
    assertEquals(algo.next(7.0), 4.0)
  }

  test("decrease") {
    val algo = OnlineDerivative(Double.NaN)
    assert(algo.next(10.0).isNaN)
    assertEquals(algo.next(9.0), -1.0)
    assertEquals(algo.next(8.0), -1.0)
    assertEquals(algo.next(4.0), -4.0)
  }

  test("reset") {
    val algo = OnlineDerivative(Double.NaN)
    assert(algo.next(1.0).isNaN)
    assert(!algo.isEmpty)
    assertEquals(algo.next(2.0), 1.0)
    algo.reset()
    assert(algo.isEmpty)
    assert(algo.next(3.0).isNaN)
    assertEquals(algo.next(7.0), 4.0)
  }
}
