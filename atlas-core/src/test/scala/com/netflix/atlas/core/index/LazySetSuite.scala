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
package com.netflix.atlas.core.index

import java.util

import org.scalatest.FunSuite


class LazySetSuite extends FunSuite {

  def testContains(s: LazySet) {
    assert(s.contains(1) === true)
    assert(s.contains(2) === true)
    assert(s.contains(3) === true)
    assert(s.contains(4) === false)
    assert(s.contains(5) === true)
    assert(s.contains(6) === false)
    assert(s.contains(7) === false)
  }

  def testIteratorNext(s: LazySet) {
    val values = Array(1, 2, 3, 5)
    val it = s.iterator
    values.foreach { v =>
      assert(it.hasNext)
      assert(it.next() === v)
    }
    assert(!it.hasNext)
  }

  def testIteratorSkipTo(s: LazySet) {
    val it = s.iterator
    assert(it.skipTo(1) === true)
    assert(it.next() === 2)
    assert(it.skipTo(4) === false)
    assert(it.skipTo(5) === true)
    assert(it.hasNext === false)
  }

  def test(s: LazySet) {
    val values = Array(1, 2, 3, 5)
    val it = s.iterator
    values.foreach { v =>
      assert(it.hasNext)
      assert(it.next() === v)
    }
    assert(!it.hasNext)
  }

  test("SortedArraySet contains") {
    val s = new SortedArraySet(Array(1, 2, 3, 5, 7), 4)
    testContains(s)
  }

  test("BitMaskSet contains") {
    val m = new util.BitSet
    Array(1, 2, 3, 5).foreach { v => m.set(v) }
    val s = new BitMaskSet(m)
    testContains(s)
  }

  test("SortedArraySet iterator next") {
    val s = new SortedArraySet(Array(1, 2, 3, 5, 7), 4)
    testIteratorNext(s)
  }

  test("BitMaskSet iterator next") {
    val m = new util.BitSet
    Array(1, 2, 3, 5).foreach { v => m.set(v) }
    val s = new BitMaskSet(m)
    testIteratorNext(s)
  }

  test("SortedArraySet iterator skipTo") {
    val s = new SortedArraySet(Array(1, 2, 3, 5, 7), 4)
    testIteratorSkipTo(s)
  }

  test("BitMaskSet iterator skipTo") {
    val m = new util.BitSet
    Array(1, 2, 3, 5).foreach { v => m.set(v) }
    val s = new BitMaskSet(m)
    testIteratorSkipTo(s)
  }

  test("set offset") {
    val s = LazySet(1, 2, 3, 4)
    val expected = List(3, 4)
    assert(s.offset(2).toList === expected)
  }

  test("set intersection") {
    val s1 = LazySet(1, 2, 3, 4)
    val s2 = LazySet(1, 2, 3, 5)
    val expected = List(1, 2, 3)
    assert(s1.intersect(s2).toList === expected)
  }

  test("set intersection 2") {
    val s1 = LazySet(1, 2, 3, 4)
    val s2 = LazySet(1, 2, 3, 5)
    val s3 = LazySet(3)
    val expected = List(3)
    assert(s3.intersect(s1.intersect(s2)).toList === expected)
    assert(s1.intersect(s2).intersect(s3).toList === expected)
  }

  test("set intersection empty") {
    val s1 = LazySet.empty
    val s2 = LazySet(1, 2, 3, 5)
    val expected = List.empty
    assert(s1.intersect(s2).toList === expected)
  }

  test("set union") {
    val s1 = LazySet(1, 2, 3, 4)
    val s2 = LazySet(1, 2, 3, 5)
    val expected = List(1, 2, 3, 4, 5)
    assert(s1.union(s2).toList === expected)
  }

  test("set union different sizes") {
    val s1 = LazySet(1, 2, 3, 4)
    val s2 = LazySet(3)
    val expected = List(1, 2, 3, 4)
    assert(s1.union(s2).toList === expected)
  }

  test("set union self") {
    val s1 = LazySet(1, 2, 3, 4)
    val s2 = LazySet(1, 2, 3, 4)
    val expected = List(1, 2, 3, 4)
    assert(s1.union(s2).toList === expected)
  }

  test("set union 2") {
    val s1 = LazySet(1, 2, 3, 4)
    val s2 = LazySet(1, 2, 3, 5)
    val s3 = LazySet(3)
    val expected = List(1, 2, 3, 4, 5)
    assert(s3.union(s1.union(s2)).toList === expected)
    assert(s1.union(s2).union(s3).toList === expected)
  }

  test("set union empty") {
    val s1 = LazySet.empty
    val s2 = LazySet(1, 2, 3, 5)
    val expected = List(1, 2, 3, 5)
    assert(s1.union(s2).toList === expected)
  }

  test("set union disjoint") {
    val s1 = LazySet(87, 93, 500)
    val s2 = LazySet(1, 2, 3, 5)
    val expected = List(1, 2, 3, 5, 87, 93, 500)
    assert(s1.union(s2).toList === expected)
  }

  test("set diff") {
    val s1 = LazySet(1, 2, 3, 4)
    val s2 = LazySet(1, 2, 5)
    val expected = List(3, 4)
    assert(s1.diff(s2).toList === expected)
  }

  test("set diff: BitMaskSet") {
    val s1 = LazySet.all(4)
    val s2 = LazySet(1, 2, 510)
    val expected = List(0, 3)
    assert(s1.diff(s2).toList === expected)
  }

  test("intersect union") {
    val s1 = LazySet(87, 93, 500)
    val s2 = LazySet(1, 2, 3, 5)
    val s3 = LazySet(87, 7)
    val expected = List(87)
    assert(s3.intersect(s1.union(s2)).toList === expected)
    assert(s1.union(s2).intersect(s3).toList === expected)
  }
}
