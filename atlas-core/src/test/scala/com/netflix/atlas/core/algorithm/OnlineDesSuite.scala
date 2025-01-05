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

class OnlineDesSuite extends BaseOnlineAlgorithmSuite {

  override def newInstance: OnlineAlgorithm = OnlineDes(10, 0.1, 0.2)

  test("training = 2, NaNs") {
    val algo = OnlineDes(2, 0.25, 0.25)
    assert(algo.next(0.0).isNaN)
    assert(algo.next(1.0).isNaN)
    algo.next(2.0)
    algo.next(Double.NaN)
    assert(!algo.isEmpty)
    algo.next(Double.NaN)
    assert(algo.isEmpty)
  }
}
