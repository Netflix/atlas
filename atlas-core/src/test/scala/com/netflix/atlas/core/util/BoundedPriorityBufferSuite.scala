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
package com.netflix.atlas.core.util

import org.scalatest.funsuite.AnyFunSuite

import java.util.Comparator

class BoundedPriorityBufferSuite extends AnyFunSuite {

  test("buffer is bounded, natural order") {
    val buffer = new BoundedPriorityBuffer[Integer](5, Comparator.naturalOrder[Integer]())
    (0 until 10).foreach(v => buffer.add(v))
    assert(buffer.toList.toSet === (0 until 5).toSet)
    assert(buffer.size == 5)
  }

  test("buffer is bounded, reversed order") {
    val cmp = Comparator.naturalOrder[Integer]().reversed()
    val buffer = new BoundedPriorityBuffer[Integer](5, cmp)
    (0 until 10).foreach(v => buffer.add(v))
    assert(buffer.toList.toSet === (5 until 10).toSet)
    assert(buffer.size == 5)
  }

  test("duplicate values, prefer first to come") {
    val cmp: Comparator[(Integer, Integer)] = (a, b) => Integer.compare(a._1, b._1)
    val buffer = new BoundedPriorityBuffer[(Integer, Integer)](5, cmp)
    (0 until 10).foreach(v => buffer.add(1.asInstanceOf[Integer] -> (9 - v).asInstanceOf[Integer]))
    assert(buffer.toList.map(_._2).toSet === (5 until 10).toSet)
  }

  test("foreach") {
    val buffer = new BoundedPriorityBuffer[Integer](3, Comparator.naturalOrder[Integer]())
    (0 until 10).foreach(v => buffer.add(v))
    val builder = Set.newBuilder[Integer]
    buffer.foreach(v => builder += v)
    assert(builder.result() === Set(0, 1, 2))
  }

  test("max size of 0") {
    intercept[IllegalArgumentException] {
      new BoundedPriorityBuffer[Integer](0, Comparator.naturalOrder[Integer]())
    }
  }

  test("negative max size") {
    intercept[IllegalArgumentException] {
      new BoundedPriorityBuffer[Integer](-3, Comparator.naturalOrder[Integer]())
    }
  }

  test("ejected value") {
    val buffer = new BoundedPriorityBuffer[Integer](2, Comparator.naturalOrder[Integer]())
    assert(buffer.add(2) === null)
    assert(buffer.add(3) === null)
    assert(buffer.add(1) === 3)
    assert(buffer.add(4) === 4)
    assert(buffer.add(0) === 2)
  }
}
