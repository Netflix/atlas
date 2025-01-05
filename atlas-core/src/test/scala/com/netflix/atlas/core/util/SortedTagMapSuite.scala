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

import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Tag
import munit.FunSuite

class SortedTagMapSuite extends FunSuite {

  test("empty") {
    val m = SortedTagMap.empty
    assert(m.isEmpty)
    assert(m.size == 0)
    assert(m.get("a").isEmpty)
    assert(!m.contains("a"))
    assertEquals(m.toList, List.empty)
    assertEquals(m.keysArray.toSeq, Seq.empty)
    assertEquals(m.valuesArray.toSeq, Seq.empty)
  }

  test("single pair") {
    val m = SortedTagMap(Array("a", "1"))
    assert(m.nonEmpty)
    assert(m.size == 1)
    assert(m.get("a").contains("1"))
    assert(m.contains("a"))
    assertEquals(m.toList, List("a" -> "1"))
    assertEquals(m.keysArray.toSeq, Seq("a"))
    assertEquals(m.valuesArray.toSeq, Seq("1"))
  }

  test("four pairs") {
    val m = SortedTagMap(Array("c", "2", "a", "0", "d", "3", "b", "1"))
    assert(m.nonEmpty)
    assert(m.size == 4)
    (0 until 4).foreach { i =>
      val k = s"${('a' + i).asInstanceOf[Char]}"
      assert(m.get(k).contains(i.toString))
    }
    assertEquals(m.toList, List("a" -> "0", "b" -> "1", "c" -> "2", "d" -> "3"))
    assertEquals(m.keysArray.toSeq, Seq("a", "b", "c", "d"))
    assertEquals(m.valuesArray.toSeq, Seq("0", "1", "2", "3"))
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
    assertEquals(m.toList, pairs)
  }

  test("apply: array with duplicates") {
    val actual = SortedTagMap(Array("a", "1", "b", "2", "a", "3"))
    val expected = SortedTagMap("a" -> "3", "b" -> "2")
    assertEquals(actual, expected)
  }

  test("apply: varargs tuples with duplicates") {
    val actual = SortedTagMap("a" -> "1", "b" -> "2", "a" -> "3")
    val expected = SortedTagMap("a" -> "3", "b" -> "2")
    assertEquals(actual, expected)
  }

  test("apply: varargs tuples with duplicates") {
    val actual = SortedTagMap(List("a" -> "1", "b" -> "2", "a" -> "3"))
    val expected = SortedTagMap("a" -> "3", "b" -> "2")
    assertEquals(actual, expected)
  }

  test("equals") {
    val input = scala.util.Random.shuffle(pairs)
    val actual: Map[String, String] = SortedTagMap(input)
    val expected: Map[String, String] = Map.empty[String, String] ++ input
    assertEquals(actual, expected)
  }

  test("updates: adding new keys") {
    val input = scala.util.Random.shuffle(pairs)
    val actual = input.foldLeft[Map[String, String]](SortedTagMap.empty) { (acc, t) =>
      acc + t
    }
    val expected = SortedTagMap(input)
    assertEquals(actual, expected)
    assert(actual.isInstanceOf[SortedTagMap])
  }

  test("updates: overwriting keys") {
    val input = scala.util.Random.shuffle(pairs)
    val base = SortedTagMap(input.map(t => t._1 -> "default"))
    val actual = input.foldLeft[Map[String, String]](base) { (acc, t) =>
      acc + t
    }
    val expected = SortedTagMap(input)
    assertEquals(actual, expected)
    assert(actual.isInstanceOf[SortedTagMap])
  }

  test("updates: combining maps") {
    val m1 = SortedTagMap("a" -> "1", "b" -> "2", "c" -> "3")
    val m2 = SortedTagMap("a" -> "4", "b" -> "5", "d" -> "6")
    val expected = SortedTagMap("a" -> "4", "b" -> "5", "c" -> "3", "d" -> "6")
    assertEquals(m1 ++ m2, expected)
  }

  @scala.annotation.tailrec
  private def removalTest(input: List[(String, String)]): Unit = {
    if (input.nonEmpty) {
      // remove key that exists
      val actual = SortedTagMap(input) - input.head._1
      val expected = SortedTagMap(input.tail)
      assertEquals(actual, expected)

      // remove key that is missing
      assertEquals(actual - input.head._1, expected)

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
    assertEquals(actual.result(), expected)
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
    assertEquals(actual, expected)
  }

  test("create from IterableOnce") {
    val map = pairs.toMap.view
    val actual = SortedTagMap(map)
    val expected = SortedTagMap(pairs)
    assertEquals(actual, expected)
  }

  test("create uneven size") {
    intercept[IllegalArgumentException] {
      SortedTagMap(Array("a", "b", "c"))
    }
  }

  test("compareTo: empty") {
    val a = SortedTagMap.empty
    val b = SortedTagMap(List.empty)
    assertEquals(a.compareTo(b), 0)
    assertEquals(b.compareTo(a), 0)
  }

  test("compareTo: self") {
    val a = SortedTagMap(Array("a", "1", "b", "2"))
    assertEquals(a.compareTo(a), 0)
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

  test("builder: duplicates result") {
    val m1 = SortedTagMap("a" -> "1", "b" -> "2", "c" -> "3")
    val m2 = SortedTagMap("a" -> "4", "b" -> "5", "d" -> "6")
    val actual = SortedTagMap
      .builder()
      .addAll(m1)
      .addAll(m2)
      .result()
    val expected = SortedTagMap("a" -> "4", "b" -> "5", "c" -> "3", "d" -> "6")
    assertEquals(actual, expected)
  }

  test("builder: duplicates compact") {
    val m1 = SortedTagMap("a" -> "1", "b" -> "2", "c" -> "3")
    val m2 = SortedTagMap("a" -> "4", "b" -> "5", "d" -> "6")
    val actual = SortedTagMap
      .builder()
      .addAll(m1)
      .addAll(m2)
      .result()
    val expected = SortedTagMap("a" -> "4", "b" -> "5", "c" -> "3", "d" -> "6")
    assertEquals(actual, expected)
  }

  test("builder: compact result") {
    val expected = SortedTagMap
      .builder(50)
      .add("a", "1")
      .add("b", "2")
      .result()
    val actual = SortedTagMap
      .builder(50)
      .add("a", "1")
      .add("b", "2")
      .compact()
    assertEquals(actual, expected)
    assertEquals(actual.backingArraySize, 4)
  }

  test("builder: compact result, no change") {
    val expected = SortedTagMap("a" -> "1", "b" -> "2")
    val actual = SortedTagMap
      .builder(2)
      .add("a", "1")
      .add("b", "2")
      .compact()
    assertEquals(actual, expected)
    assertEquals(actual.backingArraySize, 4)
  }

  test("equals contract") {
    val a = SortedTagMap(Array("a", "1", "b", "2"))
    val b = SortedTagMap(Array("a", "1", "b", "2", "c", "3"))
    val c = SortedTagMap.createUnsafe(Array("a", "1", "b", "2", null, null), 4)
    val d = SortedTagMap.createUnsafe(Array("a", "1", "b", "2", null, null, null, null), 4)

    // reflexive
    assertEquals(a, a)
    assertEquals(b, b)
    assertEquals(c, c)
    assertEquals(d, d)

    // symmetric
    assert(a != b && b != a)
    assert(a == c && c == a)
    assert(a == d && c == d)

    // transitive
    assert(a == c && c == d && a == d)

    // nulls
    assert(a != null)
  }

  test("hashCode") {
    val a = SortedTagMap(Array("a", "1", "b", "2"))
    val b = SortedTagMap(Array("a", "1", "b", "2", "c", "3"))
    val c = SortedTagMap.createUnsafe(Array("a", "1", "b", "2", null, null), 4)
    val d = SortedTagMap.createUnsafe(Array("a", "1", "b", "2", null, null, null, null), 4)

    assertEquals(a.hashCode, a.hashCode)
    assertEquals(b.hashCode, b.hashCode)
    assertEquals(c.hashCode, c.hashCode)
    assertEquals(d.hashCode, d.hashCode)

    assertNotEquals(a.hashCode, b.hashCode)
    assertEquals(a.hashCode, c.hashCode)
    assertEquals(a.hashCode, d.hashCode)
  }

  test("toSpectatorId: empty") {
    intercept[IllegalArgumentException] {
      SortedTagMap.empty.toSpectatorId()
    }
  }

  test("toSpectatorId: missing name") {
    intercept[IllegalArgumentException] {
      SortedTagMap("a" -> "1", "b" -> "2").toSpectatorId()
    }
  }

  test("toSpectatorId: missing name and default") {
    val id = SortedTagMap("a" -> "1", "b" -> "2").toSpectatorId(Option("dflt"))
    val expected = Id.create("dflt").withTags("a", "1", "b", "2")
    assertEquals(id, expected)
  }

  test("toSpectatorId: name only") {
    val id = SortedTagMap("name" -> "foo").toSpectatorId()
    assertEquals(id.name(), "foo")
    assertEquals(id.size(), id.size())
    assertEquals(id.getKey(0), "name")
    assertEquals(id.getValue(0), "foo")
    assert(!id.tags().iterator().hasNext)
  }

  test("toSpectatorId: explicit name and default") {
    val id = SortedTagMap("name" -> "foo").toSpectatorId(Option("dflt"))
    assertEquals(id.name(), "foo")
    assertEquals(id.size(), id.size())
    assertEquals(id.getKey(0), "name")
    assertEquals(id.getValue(0), "foo")
    assert(!id.tags().iterator().hasNext)
  }

  test("toSpectatorId: name and tags") {
    val map = SortedTagMap("name" -> "foo", "a" -> "1", "b" -> "2", "p" -> "3", "q" -> "4")
    val id = map.toSpectatorId()
    assertEquals(id.name(), "foo")
    assertEquals(id.size(), id.size())
    assertEquals(id.getKey(0), "name")
    assertEquals(id.getValue(0), "foo")

    var expectedTuples = List("a" -> "1", "b" -> "2", "p" -> "3", "q" -> "4")
    var i = 1
    val it = id.tags().iterator()
    while (it.hasNext) {
      val t = it.next()
      val expected = expectedTuples.head
      expectedTuples = expectedTuples.tail
      assert("name" != t.key())
      assertEquals(expected._1, t.key())
      assertEquals(expected._2, t.value())
      assertEquals(expected._1, id.getKey(i))
      assertEquals(expected._2, id.getValue(i))
      i += 1
    }
  }

  test("toSpectatorId: withTag") {
    val map = SortedTagMap("name" -> "foo", "a" -> "1", "b" -> "2", "p" -> "3", "q" -> "4")
    val id = map.toSpectatorId().withTag("r", "5").withTag(Tag.of("s", "6"))
    assertEquals(id.name(), "foo")
    assertEquals(id.size(), id.size())
    assertEquals(id.getKey(0), "name")
    assertEquals(id.getValue(0), "foo")

    var expectedTuples =
      List("a" -> "1", "b" -> "2", "p" -> "3", "q" -> "4", "r" -> "5", "s" -> "6")
    var i = 1
    val it = id.tags().iterator()
    while (it.hasNext) {
      val t = it.next()
      val expected = expectedTuples.head
      expectedTuples = expectedTuples.tail
      assert("name" != t.key())
      assertEquals(expected._1, t.key())
      assertEquals(expected._2, t.value())
      assertEquals(expected._1, id.getKey(i))
      assertEquals(expected._2, id.getValue(i))
      i += 1
    }
  }
}
