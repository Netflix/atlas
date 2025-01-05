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

import org.openjdk.jol.info.ClassLayout
import org.openjdk.jol.info.GraphLayout
import munit.FunSuite

import scala.util.Random

class RefDoubleHashMapSuite extends FunSuite {

  import java.lang.Long as JLong

  test("put") {
    val m = new RefDoubleHashMap[JLong]
    assertEquals(0, m.size)
    m.put(11L, 42)
    assertEquals(1, m.size)
    assertEquals(Map(JLong.valueOf(11) -> 42.0), m.toMap)
  }

  test("putIfAbsent") {
    val m = new RefDoubleHashMap[JLong]
    assertEquals(0, m.size)
    assert(m.putIfAbsent(11L, 42))
    assertEquals(1, m.size)
    assertEquals(Map(JLong.valueOf(11) -> 42.0), m.toMap)

    assert(!m.putIfAbsent(11L, 43))
    assertEquals(1, m.size)
    assertEquals(Map(JLong.valueOf(11) -> 42.0), m.toMap)
  }

  test("get") {
    val m = new RefDoubleHashMap[JLong]
    assertEquals(m.get(42L, -1), -1.0)
    m.put(11L, 27)
    assertEquals(m.get(42L, -1), -1.0)
    assertEquals(m.get(11L, -1), 27.0)
  }

  test("get - collisions") {
    // Underlying capacity will be 11, next prime after 10, so 0 and multiples of 11
    // will collide
    val m = new RefDoubleHashMap[JLong]
    m.put(0L, 0)
    m.put(11L, 1)
    m.put(22L, 2)
    assertEquals(m.size, 3)
    assertEquals(m.get(0L, -1), 0.0)
    assertEquals(m.get(11L, -1), 1.0)
    assertEquals(m.get(22L, -1), 2.0)
  }

  test("dedup") {
    val m = new RefDoubleHashMap[JLong]
    m.put(42L, 1)
    assertEquals(Map(JLong.valueOf(42) -> 1.0), m.toMap)
    assertEquals(1, m.size)
    m.put(42L, 2)
    assertEquals(Map(JLong.valueOf(42) -> 2.0), m.toMap)
    assertEquals(1, m.size)
  }

  test("add") {
    val m = new RefDoubleHashMap[JLong]
    assertEquals(0, m.size)

    m.add(42L, 1.0)
    assertEquals(1, m.size)
    assertEquals(Map(JLong.valueOf(42) -> 1.0), m.toMap)

    m.add(42L, 1.0)
    assertEquals(1, m.size)
    assertEquals(Map(JLong.valueOf(42) -> 2.0), m.toMap)

    m.add(42L, 7.0)
    assertEquals(1, m.size)
    assertEquals(Map(JLong.valueOf(42) -> 9.0), m.toMap)
  }

  test("add - collisions") {
    // Underlying capacity will be 11, next prime after 10, so 0 and multiples of 11
    // will collide
    val m = new RefDoubleHashMap[JLong]
    m.add(0L, 1.0)
    m.add(11L, 1.0)
    m.add(22L, 1.0)
    assertEquals(m.size, 3)
    m.foreach { (_, v) =>
      assertEquals(v, 1.0)
    }
  }

  test("max") {
    val m = new RefDoubleHashMap[JLong]
    assertEquals(0, m.size)

    m.max(42L, 1.0)
    assertEquals(1, m.size)
    assertEquals(Map(JLong.valueOf(42) -> 1.0), m.toMap)

    m.max(42L, 0.5)
    assertEquals(1, m.size)
    assertEquals(Map(JLong.valueOf(42) -> 1.0), m.toMap)

    m.max(42L, 7.0)
    assertEquals(1, m.size)
    assertEquals(Map(JLong.valueOf(42) -> 7.0), m.toMap)
  }

  test("min") {
    val m = new RefDoubleHashMap[JLong]
    assertEquals(0, m.size)

    m.min(42L, 1.0)
    assertEquals(1, m.size)
    assertEquals(Map(JLong.valueOf(42) -> 1.0), m.toMap)

    m.min(42L, 0.5)
    assertEquals(1, m.size)
    assertEquals(Map(JLong.valueOf(42) -> 0.5), m.toMap)

    m.min(42L, 7.0)
    assertEquals(1, m.size)
    assertEquals(Map(JLong.valueOf(42) -> 0.5), m.toMap)
  }

  test("mapToArray") {
    val m = new RefDoubleHashMap[JLong]
    m.add(0L, 1.0)
    m.add(11L, 1.0)
    m.add(22L, 1.0)
    val data = m.mapToArray(new Array[Double](m.size)) { (k, v) =>
      k + v
    }
    assertEquals(data.toList, List(1.0, 12.0, 23.0))
  }

  test("mapToArray -- invalid length") {
    val m = new RefDoubleHashMap[JLong]
    m.add(0L, 1.0)
    m.add(11L, 1.0)
    m.add(22L, 1.0)
    intercept[IllegalArgumentException] {
      m.mapToArray(new Array[Double](0)) { (k, v) =>
        k + v
      }
    }
  }

  test("resize") {
    val m = new RefDoubleHashMap[JLong]
    (0 until 10000).foreach(i => m.put(i.toLong, i.toDouble))
    assertEquals((0 until 10000).map(i => JLong.valueOf(i) -> i.toDouble).toMap, m.toMap)
  }

  test("resize - add") {
    val m = new RefDoubleHashMap[JLong]
    (0 until 10000).foreach(i => m.add(i.toLong, i.toDouble))
    assertEquals((0 until 10000).map(i => JLong.valueOf(i) -> i.toDouble).toMap, m.toMap)
  }

  test("random") {
    val jmap = new scala.collection.mutable.HashMap[JLong, Double]
    val imap = new RefDoubleHashMap[JLong]
    (0 until 10000).foreach { i =>
      val v = Random.nextInt()
      imap.put(v.toLong, i)
      jmap.put(v.toLong, i)
    }
    assertEquals(jmap.toMap, imap.toMap)
    assertEquals(jmap.size, imap.size)
  }

  test("memory per map") {
    // Sanity check to verify if some change introduces more overhead per set
    val bytes = ClassLayout.parseClass(classOf[RefDoubleHashMap[JLong]]).instanceSize()
    assertEquals(bytes, 32L)
  }

  test("memory - 5 items") {
    val imap = new RefDoubleHashMap[JLong]
    val jmap = new java.util.HashMap[Long, Double](10)
    (0 until 5).foreach { i =>
      imap.put(i.toLong, i)
      jmap.put(i, i)
    }

    val igraph = GraphLayout.parseInstance(imap)
    // val jgraph = GraphLayout.parseInstance(jmap)

    // println(igraph.toFootprint)
    // println(jgraph.toFootprint)

    // Only objects should be the key/value arrays and the map itself + 5 key objects
    assertEquals(igraph.totalCount(), 8L)

    // Sanity check size is < 400 bytes
    assert(igraph.totalSize() <= 400)
  }

  test("memory - 10k items") {
    val imap = new RefDoubleHashMap[JLong]
    val jmap = new java.util.HashMap[Long, Double](10)
    (0 until 10000).foreach { i =>
      imap.put(i.toLong, i)
      jmap.put(i, i)
    }

    val igraph = GraphLayout.parseInstance(imap)
    // val jgraph = GraphLayout.parseInstance(jmap)

    // println(igraph.toFootprint)
    // println(jgraph.toFootprint)

    // Only objects should be the key/value arrays and the map itself + 10000 key objects
    assertEquals(igraph.totalCount(), 3L + 10000)

    // Sanity check size is < 600kb
    assert(igraph.totalSize() <= 600000)
  }

  test("negative absolute value") {
    val s = new RefDoubleHashMap[RefDoubleHashMapSuite.MinHash]()
    assertEquals(s.get(new RefDoubleHashMapSuite.MinHash, 0.0), 0.0)
  }
}

object RefDoubleHashMapSuite {

  class MinHash {

    override def hashCode: Int = Integer.MIN_VALUE
  }
}
