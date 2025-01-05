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

class DoubleIntHashMapSuite extends FunSuite {

  test("put") {
    val m = new DoubleIntHashMap
    assertEquals(0, m.size)
    m.put(11.0, 42)
    assertEquals(1, m.size)
    assertEquals(Map(11.0 -> 42), m.toMap)
  }

  test("get") {
    val m = new DoubleIntHashMap
    assertEquals(m.get(42.0, -1), -1)
    m.put(11.0, 27)
    assertEquals(m.get(42.0, -1), -1)
    assertEquals(m.get(11.0, -1), 27)
  }

  test("dedup") {
    val m = new DoubleIntHashMap
    m.put(42.0, 1)
    assertEquals(Map(42.0 -> 1), m.toMap)
    assertEquals(1, m.size)
    m.put(42.0, 2)
    assertEquals(Map(42.0 -> 2), m.toMap)
    assertEquals(1, m.size)
  }

  test("increment") {
    val m = new DoubleIntHashMap
    assertEquals(0, m.size)

    m.increment(42.0)
    assertEquals(1, m.size)
    assertEquals(Map(42.0 -> 1), m.toMap)

    m.increment(42.0)
    assertEquals(1, m.size)
    assertEquals(Map(42.0 -> 2), m.toMap)

    m.increment(42.0, 7)
    assertEquals(1, m.size)
    assertEquals(Map(42.0 -> 9), m.toMap)
  }

  test("resize") {
    val m = new DoubleIntHashMap
    (0 until 10000).foreach(i => m.put(i, i))
    assertEquals((0 until 10000).map(i => i.toDouble -> i).toMap, m.toMap)
  }

  test("resize - increment") {
    val m = new DoubleIntHashMap
    (0 until 10000).foreach(i => m.increment(i, i))
    assertEquals((0 until 10000).map(i => i.toDouble -> i).toMap, m.toMap)
  }

  test("random") {
    val jmap = new scala.collection.mutable.HashMap[Double, Int]
    val imap = new DoubleIntHashMap
    (0 until 10000).foreach { i =>
      val v = Random.nextInt()
      imap.put(v, i)
      jmap.put(v, i)
    }
    assertEquals(jmap.toMap, imap.toMap)
    assertEquals(jmap.size, imap.size)
  }

  test("memory per map") {
    // Sanity check to verify if some change introduces more overhead per set
    val bytes = ClassLayout.parseClass(classOf[DoubleIntHashMap]).instanceSize()
    assertEquals(bytes, 16L)
  }

  test("memory - 5 items") {
    val imap = new DoubleIntHashMap
    val jmap = new java.util.HashMap[Double, Int](10)
    (0 until 5).foreach { i =>
      imap.put(i, i)
      jmap.put(i, i)
    }

    val igraph = GraphLayout.parseInstance(imap)
    // val jgraph = GraphLayout.parseInstance(jmap)

    // println(igraph.toFootprint)
    // println(jgraph.toFootprint)

    // Only objects should be the key/value arrays and the map itself
    assertEquals(igraph.totalCount(), 4L)

    // Sanity check size is < 300 bytes
    assert(igraph.totalSize() <= 300)
  }

  test("memory - 10k items") {
    val imap = new DoubleIntHashMap
    val jmap = new java.util.HashMap[Double, Int](10)
    (0 until 10000).foreach { i =>
      imap.put(i, i)
      jmap.put(i, i)
    }

    val igraph = GraphLayout.parseInstance(imap)
    // val jgraph = GraphLayout.parseInstance(jmap)

    // println(igraph.toFootprint)
    // println(jgraph.toFootprint)

    // Only objects should be the key/value arrays and the map itself
    assertEquals(igraph.totalCount(), 4L)

    // Sanity check size is < 320kb
    assert(igraph.totalSize() <= 320000)
  }

  test("negative absolute value") {
    // hashes to Integer.MIN_VALUE causing: java.lang.ArrayIndexOutOfBoundsException: -2
    //
    // scala> math.abs(java.lang.Long.hashCode(java.lang.Double.doubleToLongBits(0.778945326637231)))
    // res9: Int = -2147483648
    // scala> math.abs(java.lang.Long.hashCode(java.lang.Double.doubleToLongBits(0.17321881641359504)))
    // res10: Int = -2147483648
    // scala> math.abs(java.lang.Long.hashCode(java.lang.Double.doubleToLongBits(0.4182373879985505)))
    // res11: Int = -2147483648
    val m = new DoubleIntHashMap
    assertEquals(m.get(0.778945326637231, 0), 0)
  }

}
