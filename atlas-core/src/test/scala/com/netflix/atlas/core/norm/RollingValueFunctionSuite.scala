/*
 * Copyright 2014-2021 Netflix, Inc.
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

import org.scalatest.funsuite.AnyFunSuite

class RollingValueFunctionSuite extends AnyFunSuite {

  private def newFunction(step: Long) = {
    val listVF = new ListValueFunction
    val normalizeVF = new RollingValueFunction(step, (_, v) => v, listVF)
    listVF.f = normalizeVF
    listVF
  }

  test("values received in order") {
    val f = newFunction(1L)
    (0 until 20).foreach { i =>
      val vs = f.update(i, i)
      assert(vs == List(i -> i.toDouble))
    }
    f.close()
    assert(f.result() === Nil)
  }

  test("values received out of order") {
    val f = newFunction(1L)
    assert(f.update(1, 1.0) === List(1 -> 1.0))
    assert(f.update(2, 2.0) === List(2 -> 2.0))
    assert(f.update(1, 0.5) === List(1 -> 0.5))
    assert(f.update(3, 3.0) === List(3 -> 3.0))
    assert(f.update(1, 0.0) === Nil) // too old
    f.close()
    assert(f.result() === Nil)
  }

  test("values with gaps") {
    val f = newFunction(1L)
    assert(f.update(1, 1.0) === List(1 -> 1.0))
    assert(f.update(5, 5.0) === List(5 -> 5.0))
    assert(f.update(6, 6.0) === List(6 -> 6.0))
    assert(f.update(9, 9.0) === List(9 -> 9.0))
    f.close()
    assert(f.result() === Nil)
  }
}
