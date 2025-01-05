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
package com.netflix.atlas.core.util

import munit.FunSuite

import java.util.Comparator

class BoundedPriorityBufferSuite extends FunSuite {

  private def set(vs: Seq[Int]): Set[Integer] = {
    vs.map(Integer.valueOf).toSet
  }

  test("buffer is bounded, natural order") {
    val buffer = new BoundedPriorityBuffer[Integer](5, Comparator.naturalOrder[Integer]())
    (0 until 10).foreach(v => buffer.add(v))
    assertEquals(buffer.toList.toSet, set(0 until 5))
    assert(buffer.size == 5)
  }

  test("buffer is bounded, reversed order") {
    val cmp = Comparator.naturalOrder[Integer]().reversed()
    val buffer = new BoundedPriorityBuffer[Integer](5, cmp)
    (0 until 10).foreach(v => buffer.add(v))
    assertEquals(buffer.toList.toSet, set(5 until 10))
    assert(buffer.size == 5)
  }

  test("duplicate values, prefer first to come") {
    val cmp: Comparator[(Integer, Integer)] = (a, b) => Integer.compare(a._1, b._1)
    val buffer = new BoundedPriorityBuffer[(Integer, Integer)](5, cmp)
    (0 until 10).foreach(v => buffer.add(1.asInstanceOf[Integer] -> (9 - v).asInstanceOf[Integer]))
    assertEquals(buffer.toList.map(_._2).toSet, set(5 until 10))
  }

  test("foreach") {
    val buffer = new BoundedPriorityBuffer[Integer](3, Comparator.naturalOrder[Integer]())
    (0 until 10).foreach(v => buffer.add(v))
    val builder = Set.newBuilder[Integer]
    buffer.foreach(v => builder += v)
    assertEquals(builder.result(), set(Seq(0, 1, 2)))
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
    assertEquals(buffer.add(2), null)
    assertEquals(buffer.add(3), null)
    assertEquals(buffer.add(1).intValue(), 3)
    assertEquals(buffer.add(4).intValue(), 4)
    assertEquals(buffer.add(0).intValue(), 2)
  }
}
