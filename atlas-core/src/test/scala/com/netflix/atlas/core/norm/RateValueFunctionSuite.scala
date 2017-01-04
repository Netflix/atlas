/*
 * Copyright 2014-2017 Netflix, Inc.
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

import org.scalatest.FunSuite

class RateValueFunctionSuite extends FunSuite {

  private def newFunction = {
    val listVF = new ListValueFunction
    val rateVF = new RateValueFunction(listVF)
    listVF.f = rateVF
    listVF
  }

  test("basic") {
    val vf = newFunction
    assert(vf.update(5, 1.0) === Nil)
    assert(vf.update(15, 2.0) === List(15 -> 100.0))
    assert(vf.update(25, 4.0) === List(25 -> 200.0))
  }

  test("decreasing value") {
    val vf = newFunction
    assert(vf.update(5, 1.0) === Nil)
    assert(vf.update(15, 2.0) === List(15 -> 100.0))
    assert(vf.update(25, 1.0) === List(25 -> 0.0))
  }

}
