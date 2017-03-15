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
package com.netflix.atlas.core.util

import org.openjdk.jol.info.ClassLayout
import org.openjdk.jol.info.GraphLayout
import org.scalatest.FunSuite

import scala.util.Random


class IntIntHashMapSuite extends FunSuite {

  test("put") {
    val m = new IntIntHashMap(-1)
    assert(0 === m.size)
    m.put(11, 42)
    assert(1 === m.size)
    assert(Map(11 -> 42) === m.toMap)
  }

  test("get") {
    val m = new IntIntHashMap(-1)
    assert(m.get(42, -1) === -1)
    m.put(11, 27)
    assert(m.get(42, -1) === -1)
    assert(m.get(11, -1) === 27)
  }

  test("get - collisions") {
    // Underlying capacity will be 11, next prime after 10, so 0 and multiples of 11
    // will collide
    val m = new IntIntHashMap(-1, 10)
    m.put(0, 0)
    m.put(11, 1)
    m.put(22, 2)
    assert(m.size === 3)
    assert(m.get(0, -1) === 0)
    assert(m.get(11, -1) === 1)
    assert(m.get(22, -1) === 2)
  }

  test("dedup") {
    val m = new IntIntHashMap(-1)
    m.put(42, 1)
    assert(Map(42 -> 1) === m.toMap)
    assert(1 === m.size)
    m.put(42, 2)
    assert(Map(42 -> 2) === m.toMap)
    assert(1 === m.size)
  }

  test("increment") {
    val m = new IntIntHashMap(-1)
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

  test("resize") {
    val m = new IntIntHashMap(-1, 10)
    (0 until 10000).foreach(i => m.put(i, i))
    assert((0 until 10000).map(i => i -> i).toMap === m.toMap)
  }

  test("random") {
    val jmap = new scala.collection.mutable.HashMap[Int, Int]
    val imap = new IntIntHashMap(-1, 10)
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
    val bytes = ClassLayout.parseClass(classOf[IntIntHashMap]).instanceSize()
    assert(bytes === 32)
  }

  test("memory - 5 items") {
    val imap = new IntIntHashMap(-1, 10)
    val jmap = new java.util.HashMap[Int, Int](10)
    (0 until 5).foreach { i =>
      imap.put(i, i)
      jmap.put(i, i)
    }

    val igraph = GraphLayout.parseInstance(imap)
    val jgraph = GraphLayout.parseInstance(jmap)

    //println(igraph.toFootprint)
    //println(jgraph.toFootprint)

    // Only objects should be the key/value arrays and the map itself
    assert(igraph.totalCount() === 3)

    // Sanity check size is < 200 bytes
    assert(igraph.totalSize() <= 200)
  }

  test("memory - 10k items") {
    val imap = new IntIntHashMap(-1, 10)
    val jmap = new java.util.HashMap[Int, Int](10)
    (0 until 10000).foreach { i =>
      imap.put(i, i)
      jmap.put(i, i)
    }

    val igraph = GraphLayout.parseInstance(imap)
    val jgraph = GraphLayout.parseInstance(jmap)

    //println(igraph.toFootprint)
    //println(jgraph.toFootprint)

    // Only objects should be the key/value arrays and the map itself
    assert(igraph.totalCount() === 3)

    // Sanity check size is < 220kb
    assert(igraph.totalSize() <= 220000)
  }

  test("negative absolute value") {
    val s = new IntIntHashMap(-1, 10)
    assert(s.get(Integer.MIN_VALUE, 0) === 0)
  }

}
