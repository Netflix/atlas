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

class SortedTagMapSuite extends AnyFunSuite {

  test("empty") {
    val m = SortedTagMap.empty
    assert(m.isEmpty)
    assert(m.size == 0)
    assert(m.get("a").isEmpty)
    assert(!m.contains("a"))
    assert(m.toList === List.empty)
  }

  test("single pair") {
    val m = SortedTagMap(Array("a", "1"))
    assert(m.nonEmpty)
    assert(m.size == 1)
    assert(m.get("a").contains("1"))
    assert(m.contains("a"))
    assert(m.toList === List("a" -> "1"))
  }

  test("four pairs") {
    val m = SortedTagMap(Array("c", "2", "a", "0", "d", "3", "b", "1"))
    assert(m.nonEmpty)
    assert(m.size == 4)
    (0 until 4).foreach { i =>
      val k = s"${('a' + i).asInstanceOf[Char]}"
      assert(m.get(k).contains(i.toString))
    }
    assert(m.toList === List("a" -> "0", "b" -> "1", "c" -> "2", "d" -> "3"))
  }

  private def pairs: List[(String, String)] = {
    (' ' to '~').toList.map { i =>
      val k = s"${('a' + i).asInstanceOf[Char]}"
      k -> i.toString
    }
  }

  test("many pairs") {
    val input = scala.util.Random.shuffle(pairs)
    val m = SortedTagMap(input)
    assert(m.nonEmpty)
    assert(m.size == input.size)
    input.foreach { t =>
      assert(m.get(t._1).contains(t._2))
    }
    assert(m.toList === pairs)
  }

  test("updates: adding new keys") {
    val input = scala.util.Random.shuffle(pairs)
    val actual = input.foldLeft[Map[String, String]](SortedTagMap.empty) { (acc, t) =>
      acc + t
    }
    val expected = SortedTagMap(input)
    assert(actual === expected)
    assert(actual.isInstanceOf[SortedTagMap])
  }

  test("updates: overwriting keys") {
    val input = scala.util.Random.shuffle(pairs)
    val base = SortedTagMap(input.map(t => t._1 -> "default"))
    val actual = input.foldLeft[Map[String, String]](base) { (acc, t) =>
      acc + t
    }
    val expected = SortedTagMap(input)
    assert(actual === expected)
    assert(actual.isInstanceOf[SortedTagMap])
  }

  @scala.annotation.tailrec
  private def removalTest(input: List[(String, String)]): Unit = {
    if (input.nonEmpty) {
      // remove key that exists
      val actual = SortedTagMap(input) - input.head._1
      val expected = SortedTagMap(input.tail)
      assert(actual === expected)

      // remove key that is missing
      assert(actual - input.head._1 === expected)

      // check type
      assert(actual.isInstanceOf[SortedTagMap])
      removalTest(input.tail)
    }
  }

  test("removals") {
    val input = scala.util.Random.shuffle(pairs)
    removalTest(input)
  }

  test("foreachEntry") {
    val actual = List.newBuilder[(String, String)]
    SortedTagMap(pairs).foreachEntry { (k, v) =>
      actual += k -> v
    }
    val expected = pairs
    assert(actual.result() === expected)
  }

  test("create from SortedTagMap") {
    val a = SortedTagMap(pairs)
    val b = SortedTagMap(a)
    assert(a eq b)
  }

  test("create from Map[String, String]") {
    val map = pairs.toMap
    val actual = SortedTagMap(map)
    val expected = SortedTagMap(pairs)
    assert(actual === expected)
  }

  test("create from IterableOnce") {
    val map = pairs.toMap.view
    val actual = SortedTagMap(map)
    val expected = SortedTagMap(pairs)
    assert(actual === expected)
  }

  test("create uneven size") {
    intercept[IllegalArgumentException] {
      SortedTagMap(Array("a", "b", "c"))
    }
  }

  test("compareTo: empty") {
    val a = SortedTagMap.empty
    val b = SortedTagMap(List.empty)
    assert(a.compareTo(b) === 0)
    assert(b.compareTo(a) === 0)
  }

  test("compareTo: self") {
    val a = SortedTagMap(Array("a", "1", "b", "2"))
    assert(a.compareTo(a) === 0)
  }

  test("compareTo: different keys") {
    val a = SortedTagMap(Array("a", "1", "b", "2"))
    val b = SortedTagMap(Array("a", "1", "c", "2"))
    assert(a.compareTo(b) < 0)
    assert(b.compareTo(a) > 0)
  }

  test("compareTo: different values") {
    val a = SortedTagMap(Array("a", "1", "b", "2"))
    val b = SortedTagMap(Array("a", "2", "b", "2"))
    assert(a.compareTo(b) < 0)
    assert(b.compareTo(a) > 0)
  }

  test("compareTo: different sizes") {
    val a = SortedTagMap(Array("a", "1", "b", "2"))
    val b = SortedTagMap(Array("a", "1", "b", "2", "c", "3"))
    assert(a.compareTo(b) < 0)
    assert(b.compareTo(a) > 0)
  }
}
