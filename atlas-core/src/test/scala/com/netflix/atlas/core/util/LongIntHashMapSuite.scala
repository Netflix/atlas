/*
 * Copyright 2014-2019 Netflix, Inc.
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

import org.openjdk.jol.info.ClassLayout
import org.openjdk.jol.info.GraphLayout
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class LongIntHashMapSuite extends AnyFunSuite {

  test("put") {
    val m = new LongIntHashMap(-1)
    assert(0 === m.size)
    m.put(11, 42)
    assert(1 === m.size)
    assert(Map(11 -> 42) === m.toMap)
  }

  test("get") {
    val m = new LongIntHashMap(-1)
    assert(m.get(42, -1) === -1)
    m.put(11, 27)
    assert(m.get(42, -1) === -1)
    assert(m.get(11, -1) === 27)
  }

  test("get - collisions") {
    // Underlying capacity will be 11, next prime after 10, so 0 and multiples of 11
    // will collide
    val m = new LongIntHashMap(-1, 10)
    m.put(0, 0)
    m.put(11, 1)
    m.put(22, 2)
    assert(m.size === 3)
    assert(m.get(0, -1) === 0)
    assert(m.get(11, -1) === 1)
    assert(m.get(22, -1) === 2)
  }

  test("dedup") {
    val m = new LongIntHashMap(-1)
    m.put(42, 1)
    assert(Map(42 -> 1) === m.toMap)
    assert(1 === m.size)
    m.put(42, 2)
    assert(Map(42 -> 2) === m.toMap)
    assert(1 === m.size)
  }

  test("increment") {
    val m = new LongIntHashMap(-1)
    assert(0 === m.size)

    m.increment(42)
    assert(1 === m.size)
    assert(Map(42 -> 1) === m.toMap)

    m.increment(42)
    assert(1 === m.size)
    assert(Map(42 -> 2) === m.toMap)

    m.increment(42, 7)
    assert(1 === m.size)
    assert(Map(42 -> 9) === m.toMap)
  }

  test("increment - collisions") {
    // Underlying capacity will be 11, next prime after 10, so 0 and multiples of 11
    // will collide
    val m = new LongIntHashMap(-1, 10)
    m.increment(0)
    m.increment(11)
    m.increment(22)
    assert(m.size === 3)
    m.foreach { (_, v) =>
      assert(v === 1)
    }
  }

  test("resize") {
    val m = new LongIntHashMap(-1, 10)
    (0 until 10000).foreach(i => m.put(i, i))
    assert((0 until 10000).map(i => i -> i).toMap === m.toMap)
  }

  test("resize - increment") {
    val m = new LongIntHashMap(-1, 10)
    (0 until 10000).foreach(i => m.increment(i, i))
    assert((0 until 10000).map(i => i -> i).toMap === m.toMap)
  }

  test("random") {
    val jmap = new scala.collection.mutable.HashMap[Long, Int]
    val imap = new LongIntHashMap(-1, 10)
    (0 until 10000).foreach { i =>
      val v = Random.nextInt()
      imap.put(v, i)
      jmap.put(v, i)
    }
    assert(jmap.toMap === imap.toMap)
    assert(jmap.size === imap.size)
  }

  test("memory per map") {
    // Sanity check to verify if some change introduces more overhead per set
    val bytes = ClassLayout.parseClass(classOf[LongIntHashMap]).instanceSize()
    assert(bytes === 40)
  }

  test("memory - 5 items") {
    val imap = new LongIntHashMap(-1, 10)
    val jmap = new java.util.HashMap[Long, Int](10)
    (0 until 5).foreach { i =>
      imap.put(i, i)
      jmap.put(i, i)
    }

    val igraph = GraphLayout.parseInstance(imap)
    //val jgraph = GraphLayout.parseInstance(jmap)

    //println(igraph.toFootprint)
    //println(jgraph.toFootprint)

    // Only objects should be the key/value arrays and the map itself
    assert(igraph.totalCount() === 3)

    // Sanity check size is < 300 bytes
    assert(igraph.totalSize() <= 300)
  }

  test("memory - 10k items") {
    val imap = new LongIntHashMap(-1, 10)
    val jmap = new java.util.HashMap[Long, Int](10)
    (0 until 10000).foreach { i =>
      imap.put(i, i)
      jmap.put(i, i)
    }

    val igraph = GraphLayout.parseInstance(imap)
    //val jgraph = GraphLayout.parseInstance(jmap)

    //println(igraph.toFootprint)
    //println(jgraph.toFootprint)

    // Only objects should be the key/value arrays and the map itself
    assert(igraph.totalCount() === 3)

    // Sanity check size is < 320kb
    assert(igraph.totalSize() <= 320000)
  }

  test("negative absolute value") {
    // hashes to Integer.MIN_VALUE causing: java.lang.ArrayIndexOutOfBoundsException: -2
    val m = new LongIntHashMap(-1, 10)
    assert(m.get(Integer.MIN_VALUE.toLong - 1, 0) === 0)
  }

}
