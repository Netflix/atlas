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

class OnlineIntegralSuite extends BaseOnlineAlgorithmSuite {

  override def newInstance: OnlineAlgorithm = OnlineIntegral(Double.NaN)

  test("init") {
    val algo = OnlineIntegral(Double.NaN)
    assertEquals(algo.next(1.0), 1.0)
    assertEquals(algo.next(1.0), 2.0)
  }

  test("NaN values") {
    val algo = OnlineIntegral(Double.NaN)
    assertEquals(algo.next(1.0), 1.0)
    assertEquals(algo.next(1.0), 2.0)
    assertEquals(algo.next(Double.NaN), 2.0)
    assertEquals(algo.next(1.0), 3.0)
    assertEquals(algo.next(1.0), 4.0)
  }

  test("increase") {
    val algo = OnlineIntegral(Double.NaN)
    assertEquals(algo.next(1.0), 1.0)
    assertEquals(algo.next(2.0), 3.0)
    assertEquals(algo.next(3.0), 6.0)
    assertEquals(algo.next(7.0), 13.0)
  }

  test("decrease") {
    val algo = OnlineIntegral(Double.NaN)
    assertEquals(algo.next(10.0), 10.0)
    assertEquals(algo.next(9.0), 19.0)
    assertEquals(algo.next(8.0), 27.0)
    assertEquals(algo.next(4.0), 31.0)
  }

  test("reset") {
    val algo = OnlineIntegral(Double.NaN)
    assertEquals(algo.next(1.0), 1.0)
    assertEquals(algo.next(2.0), 3.0)
    algo.reset()
    assertEquals(algo.next(3.0), 3.0)
    assertEquals(algo.next(7.0), 10.0)
  }
}
