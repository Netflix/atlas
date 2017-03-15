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


class RefIntHashMapSuite extends FunSuite {

  import java.lang.{Long => JLong}

  test("put") {
    val m = new RefIntHashMap[JLong]
    assert(0 === m.size)
    m.put(11L, 42)
    assert(1 === m.size)
    assert(Map(11L -> 42) === m.toMap)
  }

  test("putIfAbsent") {
    val m = new RefIntHashMap[JLong]
    assert(0 === m.size)
    assert(m.putIfAbsent(11L, 42))
    assert(1 === m.size)
    assert(Map(11L -> 42) === m.toMap)

    assert(!m.putIfAbsent(11L, 43))
    assert(1 === m.size)
    assert(Map(11L -> 42) === m.toMap)
  }

  test("get") {
    val m = new RefIntHashMap[JLong]
    assert(m.get(42L, -1) === -1)
    m.put(11L, 27)
    assert(m.get(42L, -1) === -1)
    assert(m.get(11L, -1) === 27)
  }

  test("get - collisions") {
    // Underlying capacity will be 11, next prime after 10, so 0 and multiples of 11
    // will collide
    val m = new RefIntHashMap[JLong]
    m.put(0L, 0)
    m.put(11L, 1)
    m.put(22L, 2)
    assert(m.size === 3)
    assert(m.get(0L, -1) === 0)
    assert(m.get(11L, -1) === 1)
    assert(m.get(22L, -1) === 2)
  }

  test("dedup") {
    val m = new RefIntHashMap[JLong]
    m.put(42L, 1)
    assert(Map(42L -> 1) === m.toMap)
    assert(1 === m.size)
    m.put(42L, 2)
    assert(Map(42L -> 2) === m.toMap)
    assert(1 === m.size)
  }

  test("increment") {
    val m = new RefIntHashMap[JLong]
    assert(0 === m.size)

    m.increment(42L)
    assert(1 === m.size)
    assert(Map(42L -> 1) === m.toMap)

    m.increment(42L)
    assert(1 === m.size)
    assert(Map(42L -> 2) === m.toMap)

    m.increment(42L, 7)
    assert(1 === m.size)
    assert(Map(42L -> 9) === m.toMap)
  }

  test("increment - collisions") {
    // Underlying capacity will be 11, next prime after 10, so 0 and multiples of 11
    // will collide
    val m = new RefIntHashMap[JLong]
    m.increment(0L)
    m.increment(11L)
    m.increment(22L)
    assert(m.size === 3)
    m.foreach { (_, v) => assert(v === 1) }
  }

  test("mapToArray") {
    val m = new RefIntHashMap[JLong]
    m.increment(0L)
    m.increment(11L)
    m.increment(22L)
    val data = m.mapToArray(new Array[Long](m.size)) { (k, v) => k + v }
    assert(data.toList === List(1, 12, 23))
  }

  test("mapToArray -- invalid length") {
    val m = new RefIntHashMap[JLong]
    m.increment(0L)
    m.increment(11L)
    m.increment(22L)
    intercept[IllegalArgumentException] {
      m.mapToArray(new Array[Long](0)) { (k, v) => k + v }
    }
  }

  test("resize") {
    val m = new RefIntHashMap[JLong]
    (0 until 10000).foreach(i => m.put(i.toLong, i))
    assert((0 until 10000).map(i => i -> i).toMap === m.toMap)
  }

  test("resize - increment") {
    val m = new RefIntHashMap[JLong]
    (0 until 10000).foreach(i => m.increment(i.toLong, i))
    assert((0 until 10000).map(i => i -> i).toMap === m.toMap)
  }

  test("random") {
    val jmap = new scala.collection.mutable.HashMap[Long, Int]
    val imap = new RefIntHashMap[JLong]
    (0 until 10000).foreach { i =>
      val v = Random.nextInt()
      imap.put(v.toLong, i)
      jmap.put(v, i)
    }
    assert(jmap.toMap === imap.toMap)
    assert(jmap.size === imap.size)
  }

  test("memory per map") {
    // Sanity check to verify if some change introduces more overhead per set
    val bytes = ClassLayout.parseClass(classOf[RefIntHashMap[JLong]]).instanceSize()
    assert(bytes === 32)
  }

  test("memory - 5 items") {
    val imap = new RefIntHashMap[JLong]
    val jmap = new java.util.HashMap[Long, Int](10)
    (0 until 5).foreach { i =>
      imap.put(i.toLong, i)
      jmap.put(i, i)
    }

    val igraph = GraphLayout.parseInstance(imap)
    val jgraph = GraphLayout.parseInstance(jmap)

    //println(igraph.toFootprint)
    //println(jgraph.toFootprint)

    // Only objects should be the key/value arrays and the map itself + 5 key objects
    assert(igraph.totalCount() === 8)

    // Sanity check size is < 300 bytes
    assert(igraph.totalSize() <= 300)
  }

  test("memory - 10k items") {
    val imap = new RefIntHashMap[JLong]
    val jmap = new java.util.HashMap[Long, Int](10)
    (0 until 10000).foreach { i =>
      imap.put(i.toLong, i)
      jmap.put(i, i)
    }

    val igraph = GraphLayout.parseInstance(imap)
    val jgraph = GraphLayout.parseInstance(jmap)

    //println(igraph.toFootprint)
    //println(jgraph.toFootprint)

    // Only objects should be the key/value arrays and the map itself + 10000 key objects
    assert(igraph.totalCount() === 3 + 10000)

    // Sanity check size is < 500kb
    assert(igraph.totalSize() <= 500000)
  }

  test("negative absolute value") {
    val s = new RefIntHashMap[RefIntHashMapSuite.MinHash]()
    assert(s.get(new RefIntHashMapSuite.MinHash, 0) === 0)
  }
}

object RefIntHashMapSuite {
  class MinHash {
    override def hashCode: Int = Integer.MIN_VALUE
  }
}
