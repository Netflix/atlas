/*
 * Copyright 2014-2019 Netflix, Inc.
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

class OnlineRollingSumSuite extends BaseOnlineAlgorithmSuite {

  override def newInstance: OnlineAlgorithm = OnlineRollingSum(50, 30)

  test("n = 1, min = 1") {
    val algo = OnlineRollingSum(1, 1)
    assert(algo.next(0.0) === 0.0)
    assert(algo.next(1.0) === 1.0)
  }

  test("n = 2, min = 1") {
    val algo = OnlineRollingSum(2, 1)
    assert(algo.next(0.0) === 0.0)
    assert(algo.next(1.0) === 1.0)
    assert(algo.next(2.0) === 3.0)
  }

  test("n = 2, min = 2") {
    val algo = OnlineRollingSum(2, 2)
    assert(algo.next(0.0).isNaN)
    assert(algo.next(1.0) === 1.0)
    assert(algo.next(2.0) === 3.0)
  }

  test("n = 3, min = 2 - up and down") {
    val algo = OnlineRollingSum(3, 2)
    assert(algo.next(0.0).isNaN)
    assert(algo.next(1.0) === 1.0)
    assert(algo.next(-1.0) === 0.0)
    assert(algo.next(Double.NaN) === 0.0)
    assert(algo.next(Double.NaN).isNaN)
    assert(algo.next(0.0).isNaN)
    assert(algo.next(1.0) === 1.0)
    assert(algo.next(1.0) === 2.0)
    assert(algo.next(1.0) === 3.0)
    assert(algo.next(0.0) === 2.0)
  }

  test("n = 2, min = 1, NaNs") {
    val algo = OnlineRollingSum(2, 1)
    assert(algo.next(0.0) === 0.0)
    assert(algo.next(1.0) === 1.0)
    assert(algo.next(2.0) === 3.0)
    assert(algo.next(Double.NaN) === 2.0)
    assert(algo.next(Double.NaN).isNaN)
  }

  test("n = 2, min = 1, reset") {
    val algo = OnlineRollingSum(2, 1)
    assert(algo.next(1.0) === 1.0)
    algo.reset()
    assert(algo.next(5.0) === 5.0)
  }

  test("min < n") {
    intercept[IllegalArgumentException] {
      OnlineRollingSum(1, 2)
    }
  }

  test("min = 0") {
    intercept[IllegalArgumentException] {
      OnlineRollingSum(2, 0)
    }
  }

  test("min < 0") {
    intercept[IllegalArgumentException] {
      OnlineRollingSum(2, -4)
    }
  }
}
