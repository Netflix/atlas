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
package com.netflix.atlas.core.norm

import munit.FunSuite

class RateValueFunctionSuite extends FunSuite {

  private def newFunction = {
    val listVF = new ListValueFunction
    val rateVF = new RateValueFunction(listVF)
    listVF.f = rateVF
    listVF
  }

  test("basic, 1ms step") {
    val vf = newFunction
    assertEquals(vf.update(1L, 1.0), Nil)
    assertEquals(vf.update(2L, 2.0), List(2L -> 1000.0))
    assertEquals(vf.update(3L, 4.0), List(3L -> 2000.0))
  }

  test("basic, 5ms step") {
    val vf = newFunction
    assertEquals(vf.update(5L, 1.0), Nil)
    assertEquals(vf.update(15L, 2.0), List(15L -> 100.0))
    assertEquals(vf.update(25L, 4.0), List(25L -> 200.0))
  }

  test("decreasing value") {
    val vf = newFunction
    assertEquals(vf.update(5, 1.0), Nil)
    assertEquals(vf.update(15, 2.0), List(15L -> 100.0))
    assertEquals(vf.update(25, 1.0), List(25L -> 0.0))
  }

}
