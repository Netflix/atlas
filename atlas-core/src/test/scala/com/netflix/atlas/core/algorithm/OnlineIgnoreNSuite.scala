/*
 * Copyright 2014-2020 Netflix, Inc.
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

class OnlineIgnoreNSuite extends BaseOnlineAlgorithmSuite {

  override def newInstance: OnlineAlgorithm = OnlineIgnoreN(10)

  test("n = 1") {
    val algo = OnlineIgnoreN(1)
    assert(algo.next(0.0).isNaN)
    assert(algo.next(1.0) === 1.0)
    assert(algo.next(2.0) === 2.0)
    assert(algo.next(Double.NaN).isNaN)
  }

  test("n = 1, reset") {
    val algo = OnlineIgnoreN(1)
    assert(algo.next(0.0).isNaN)
    assert(algo.next(1.0) === 1.0)
    algo.reset()
    assert(algo.next(2.0).isNaN)
    assert(algo.next(3.0) === 3.0)
  }

  test("n = 2") {
    val algo = OnlineIgnoreN(2)
    assert(algo.next(0.0).isNaN)
    assert(algo.next(1.0).isNaN)
    assert(algo.next(2.0) === 2.0)
    assert(algo.next(Double.NaN).isNaN)
  }
}
