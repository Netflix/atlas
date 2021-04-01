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

class RollingValueBufferSuite extends AnyFunSuite {

  test("values received in order") {
    val buffer = new RollingValueBuffer(1L, 2)
    (0 until 20).foreach { i =>
      val v = buffer.set(i, i)
      assert(v === i)
    }
  }

  test("set: values received out of order") {
    val buffer = new RollingValueBuffer(1L, 2)
    assert(1.0 === buffer.set(1, 1.0))
    assert(2.0 === buffer.set(2, 2.0))
    assert(0.5 === buffer.set(1, 0.5))
    assert(3.0 === buffer.set(3, 3.0))
    assert(buffer.set(1, 0.0).isNaN) // too old
  }

  test("add: values received out of order") {
    val buffer = new RollingValueBuffer(1L, 2)
    assert(1.0 === buffer.add(1, 1.0))
    assert(2.0 === buffer.add(2, 2.0))
    assert(1.5 === buffer.add(1, 0.5))
    assert(3.0 === buffer.add(3, 3.0))
    assert(buffer.add(1, 0.0).isNaN) // too old
  }

  test("max: values received out of order") {
    val buffer = new RollingValueBuffer(1L, 2)
    assert(1.0 === buffer.max(1, 1.0))
    assert(2.0 === buffer.max(2, 2.0))
    assert(1.0 === buffer.max(1, 0.5))
    assert(3.0 === buffer.max(3, 3.0))
    assert(buffer.max(1, 0.0).isNaN) // too old
  }
}
