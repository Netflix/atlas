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

abstract class BaseOnlineAlgorithmSuite extends FunSuite {

  protected def newInstance: OnlineAlgorithm

  test("restore from state") {
    val algo = newInstance
    var restoredAlgo = OnlineAlgorithm(algo.state)
    val random = new java.util.Random()
    val n = random.nextInt(200) + 50

    (0 until 10000).foreach { i =>
      val v = if (random.nextDouble() < 0.01) Double.NaN else random.nextDouble()
      val expected = algo.next(v)
      val actual = restoredAlgo.next(v)
      if (expected.isNaN) {
        assert(actual.isNaN, s"$actual != $expected")
      } else {
        assertEqualsDouble(actual, expected, 1e-12)
      }
      if (i % n == 0) {
        restoredAlgo = OnlineAlgorithm(algo.state)
      }
    }
  }
}
