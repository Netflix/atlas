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
package com.netflix.atlas.core.norm

import munit.FunSuite

class MaxValueFunctionSuite extends FunSuite {

  private def newFunction(step: Long) = {
    val listVF = new ListValueFunction
    val normalizeVF = new MaxValueFunction(step, listVF)
    listVF.f = normalizeVF
    listVF
  }

  test("basic") {
    val n = newFunction(10)
    assertEquals(n.update(5, 1.0), List(10L -> 1.0))
    assertEquals(n.update(15, 2.0), List(20L -> 2.0))
    assertEquals(n.update(25, 2.0), List(30L -> 2.0))
    assertEquals(n.update(35, 1.0), List(40L -> 1.0))
    assertEquals(n.update(85, 1.0), List(90L -> 1.0))
    assertEquals(n.update(95, 2.0), List(100L -> 2.0))
    assertEquals(n.update(105, 2.0), List(110L -> 2.0))
    n.close()
    assertEquals(n.result(), Nil)
  }

  test("already normalized updates") {
    val n = newFunction(10)
    assertEquals(n.update(0, 1.0), List(0L -> 1.0))
    assertEquals(n.update(10, 2.0), List(10L -> 2.0))
    assertEquals(n.update(20, 3.0), List(20L -> 3.0))
    assertEquals(n.update(30, 1.0), List(30L -> 1.0))
    n.close()
    assertEquals(n.result(), Nil)
  }

  test("already normalized updates, skip 1") {
    val n = newFunction(10)
    assertEquals(n.update(0, 1.0), List(0L -> 1.0))
    assertEquals(n.update(10, 1.0), List(10L -> 1.0))
    assertEquals(n.update(30, 1.0), List(30L -> 1.0))
    n.close()
    assertEquals(n.result(), Nil)
  }

  test("already normalized updates, miss heartbeat") {
    val n = newFunction(10)
    assertEquals(n.update(0, 1.0), List(0L -> 1.0))
    assertEquals(n.update(10, 2.0), List(10L -> 2.0))
    assertEquals(n.update(30, 1.0), List(30L -> 1.0))
    assertEquals(n.update(60, 4.0), List(60L -> 4.0))
    assertEquals(n.update(70, 2.0), List(70L -> 2.0))
    n.close()
    assertEquals(n.result(), Nil)
  }

  test("random offset") {

    def t(m: Int, s: Int) = (m * 60 + s) * 1000L
    val n = newFunction(60000)
    assertEquals(n.update(t(1, 13), 1.0), List(t(2, 0) -> 1.0))
    assertEquals(n.update(t(2, 13), 1.0), List(t(3, 0) -> 1.0))
    assertEquals(n.update(t(3, 13), 1.0), List(t(4, 0) -> 1.0))
    n.close()
    assertEquals(n.result(), Nil)
  }

  test("random offset, skip 1") {

    def t(m: Int, s: Int) = (m * 60 + s) * 1000L
    val n = newFunction(60000)
    assertEquals(n.update(t(1, 13), 1.0), List(t(2, 0) -> 1.0))
    assertEquals(n.update(t(2, 13), 1.0), List(t(3, 0) -> 1.0))
    assertEquals(n.update(t(3, 13), 1.0), List(t(4, 0) -> 1.0))
    assertEquals(n.update(t(5, 13), 1.0), List(t(6, 0) -> 1.0))
    n.close()
    assertEquals(n.result(), Nil)
  }

  test("random offset, skip 2") {

    def t(m: Int, s: Int) = (m * 60 + s) * 1000L
    val n = newFunction(60000)
    assertEquals(n.update(t(1, 13), 1.0), List(t(2, 0) -> 1.0))
    assertEquals(n.update(t(2, 13), 1.0), List(t(3, 0) -> 1.0))
    assertEquals(n.update(t(3, 13), 1.0), List(t(4, 0) -> 1.0))
    assertEquals(n.update(t(6, 13), 1.0), List(t(7, 0) -> 1.0))
    n.close()
    assertEquals(n.result(), Nil)
  }

  test("random offset, skip almost 2") {

    def t(m: Int, s: Int) = (m * 60 + s) * 1000L
    val n = newFunction(60000)
    assertEquals(n.update(t(1, 13), 1.0), List(t(2, 0) -> 1.0))
    assertEquals(n.update(t(2, 13), 1.0), List(t(3, 0) -> 1.0))
    assertEquals(n.update(t(3, 13), 1.0), List(t(4, 0) -> 1.0))
    assertEquals(n.update(t(6, 5), 1.0), List(t(7, 0) -> 1.0))
    n.close()
    assertEquals(n.result(), Nil)
  }

  test("random offset, out of order") {

    def t(m: Int, s: Int) = (m * 60 + s) * 1000L
    val n = newFunction(60000)
    assertEquals(n.update(t(1, 13), 1.0), List(t(2, 0) -> 1.0))
    assertEquals(n.update(t(1, 12), 1.0), List(t(2, 0) -> 1.0))
    assertEquals(n.update(t(2, 13), 1.0), List(t(3, 0) -> 1.0))
    assertEquals(n.update(t(2, 10), 1.0), List(t(3, 0) -> 1.0))
    assertEquals(n.update(t(3, 13), 1.0), List(t(4, 0) -> 1.0))
    assertEquals(n.update(t(3, 11), 1.0), List(t(4, 0) -> 1.0))
    n.close()
    assertEquals(n.result(), Nil)
  }

  test("random offset, dual reporting") {

    def t(m: Int, s: Int) = (m * 60 + s) * 1000L
    val n = newFunction(60000)
    assertEquals(n.update(t(1, 13), 1.0), List(t(2, 0) -> 1.0))
    assertEquals(n.update(t(1, 13), 1.0), List(t(2, 0) -> 1.0))
    assertEquals(n.update(t(2, 13), 1.0), List(t(3, 0) -> 1.0))
    assertEquals(n.update(t(2, 13), 1.0), List(t(3, 0) -> 1.0))
    assertEquals(n.update(t(3, 13), 1.0), List(t(4, 0) -> 1.0))
    assertEquals(n.update(t(3, 13), 1.0), List(t(4, 0) -> 1.0))
    n.close()
    assertEquals(n.result(), Nil)
  }

  test("init, 17") {

    def t(m: Int, s: Int) = (m * 60 + s) * 1000L
    val n = newFunction(60000)
    val v = 1.0 / 60.0
    assertEquals(n.update(t(8, 17), v), List(t(9, 0) -> v))
    assertEquals(n.update(t(9, 17), 0.0), List(t(10, 0) -> 0.0))
    assertEquals(n.update(t(10, 17), 0.0), List(t(11, 0) -> 0.0))
    n.close()
    assertEquals(n.result(), Nil)
  }

  test("frequent updates") {
    val n = newFunction(10)
    assertEquals(n.update(0, 1.0), List(0L -> 1.0))
    assertEquals(n.update(2, 2.0), List(10L -> 2.0))
    assertEquals(n.update(4, 4.0), List(10L -> 4.0))
    assertEquals(n.update(8, 8.0), List(10L -> 8.0))
    assertEquals(n.update(12, 2.0), List(20L -> 2.0))
    assertEquals(n.update(40, 3.0), List(40L -> 3.0))
    n.close()
    assertEquals(n.result(), Nil)
  }

  test("multi-node updates") {
    val n = newFunction(10)

    // Node 1: if shutting down it can flush an interval early
    assertEquals(n.update(0, 1.0), List(0L -> 1.0))
    assertEquals(n.update(10, 2.0), List(10L -> 2.0))

    // Other nodes: report around the same time. Need to ensure that the flush
    // from the node shutting down doesn't block the updates from the other nodes
    assertEquals(n.update(0, 3.0), List(0L -> 3.0))
    assertEquals(n.update(0, 4.0), List(0L -> 4.0))
    assertEquals(n.update(0, 5.0), List(0L -> 5.0))

    assertEquals(n.update(10, 6.0), List(10L -> 6.0))
    assertEquals(n.update(10, 7.0), List(10L -> 7.0))
    assertEquals(n.update(10, 8.0), List(10L -> 8.0))

    n.close()
    assertEquals(n.result(), Nil)
  }
}
