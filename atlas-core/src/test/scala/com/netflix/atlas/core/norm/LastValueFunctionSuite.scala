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
package com.netflix.atlas.core.norm

import com.netflix.atlas.core.util.Assert._
import org.scalatest.funsuite.AnyFunSuite

class LastValueFunctionSuite extends AnyFunSuite {

  private def newFunction(step: Long, heartbeat: Long) = {
    val listVF = new ListValueFunction
    val normalizeVF = new LastValueFunction(step, listVF)
    listVF.f = normalizeVF
    listVF
  }

  test("basic") {
    val n = newFunction(10, 20)
    assert(n.update(5, 1.0) === List(10    -> 1.0))
    assert(n.update(15, 2.0) === List(20   -> 2.0))
    assert(n.update(25, 2.0) === List(30   -> 2.0))
    assert(n.update(35, 1.0) === List(40   -> 1.0))
    assert(n.update(85, 1.0) === List(90   -> 1.0))
    assert(n.update(95, 2.0) === List(100  -> 2.0))
    assert(n.update(105, 2.0) === List(110 -> 2.0))
  }

  test("already normalized updates") {
    val n = newFunction(10, 20)
    assert(n.update(0, 1.0) === List(0   -> 1.0))
    assert(n.update(10, 2.0) === List(10 -> 2.0))
    assert(n.update(20, 3.0) === List(20 -> 3.0))
    assert(n.update(30, 1.0) === List(30 -> 1.0))
  }

  test("already normalized updates, skip 1") {
    val n = newFunction(10, 20)
    assert(n.update(0, 1.0) === List(0   -> 1.0))
    assert(n.update(10, 1.0) === List(10 -> 1.0))
    assert(n.update(30, 1.0) === List(30 -> 1.0))
  }

  test("already normalized updates, miss heartbeat") {
    val n = newFunction(10, 20)
    assert(n.update(0, 1.0) === List(0   -> 1.0))
    assert(n.update(10, 2.0) === List(10 -> 2.0))
    assert(n.update(30, 1.0) === List(30 -> 1.0))
    assert(n.update(60, 4.0) === List(60 -> 4.0))
    assert(n.update(70, 2.0) === List(70 -> 2.0))
  }

  test("random offset") {

    def t(m: Int, s: Int) = (m * 60 + s) * 1000L
    val n = newFunction(60000, 120000)
    assert(n.update(t(1, 13), 1.0) === List(t(2, 0) -> 1.0))
    assert(n.update(t(2, 13), 1.0) === List(t(3, 0) -> 1.0))
    assert(n.update(t(3, 13), 1.0) === List(t(4, 0) -> 1.0))
  }

  test("random offset, skip 1") {

    def t(m: Int, s: Int) = (m * 60 + s) * 1000L
    val n = newFunction(60000, 120000)
    assert(n.update(t(1, 13), 1.0) === List(t(2, 0) -> 1.0))
    assert(n.update(t(2, 13), 1.0) === List(t(3, 0) -> 1.0))
    assert(n.update(t(3, 13), 1.0) === List(t(4, 0) -> 1.0))
    assert(n.update(t(5, 13), 1.0) === List(t(6, 0) -> 1.0))
  }

  test("random offset, skip 2") {

    def t(m: Int, s: Int) = (m * 60 + s) * 1000L
    val n = newFunction(60000, 120000)
    assert(n.update(t(1, 13), 1.0) === List(t(2, 0) -> 1.0))
    assert(n.update(t(2, 13), 1.0) === List(t(3, 0) -> 1.0))
    assert(n.update(t(3, 13), 1.0) === List(t(4, 0) -> 1.0))
    assert(n.update(t(6, 13), 1.0) === List(t(7, 0) -> 1.0))
  }

  test("random offset, skip almost 2") {

    def t(m: Int, s: Int) = (m * 60 + s) * 1000L
    val n = newFunction(60000, 120000)
    assert(n.update(t(1, 13), 1.0) === List(t(2, 0) -> 1.0))
    assert(n.update(t(2, 13), 1.0) === List(t(3, 0) -> 1.0))
    assert(n.update(t(3, 13), 1.0) === List(t(4, 0) -> 1.0))
    assert(n.update(t(6, 5), 1.0) === List(t(7, 0)  -> 1.0))
  }

  test("random offset, out of order") {

    def t(m: Int, s: Int) = (m * 60 + s) * 1000L
    val n = newFunction(60000, 120000)
    assert(n.update(t(1, 13), 1.0) === List(t(2, 0) -> 1.0))
    assert(n.update(t(1, 12), 1.0) === Nil)
    assert(n.update(t(2, 13), 1.0) === List(t(3, 0) -> 1.0))
    assert(n.update(t(2, 10), 1.0) === Nil)
    assert(n.update(t(3, 13), 1.0) === List(t(4, 0) -> 1.0))
    assert(n.update(t(3, 11), 1.0) === Nil)
  }

  test("random offset, dual reporting") {

    def t(m: Int, s: Int) = (m * 60 + s) * 1000L
    val n = newFunction(60000, 120000)
    assert(n.update(t(1, 13), 1.0) === List(t(2, 0) -> 1.0))
    assert(n.update(t(1, 13), 1.0) === Nil)
    assert(n.update(t(2, 13), 1.0) === List(t(3, 0) -> 1.0))
    assert(n.update(t(2, 13), 1.0) === Nil)
    assert(n.update(t(3, 13), 1.0) === List(t(4, 0) -> 1.0))
    assert(n.update(t(3, 13), 1.0) === Nil)
  }

  test("init, 17") {

    def t(m: Int, s: Int) = (m * 60 + s) * 1000L
    val n = newFunction(60000, 120000)
    val v = 1.0 / 60.0
    assert(n.update(t(8, 17), 1.0 / 60.0) === List(t(9, 0) -> v))
    assert(n.update(t(9, 17), 0.0) === List(t(10, 0)       -> 0.0))
    assert(n.update(t(10, 17), 0.0) === List(t(11, 0)      -> 0.0))
  }

  test("frequent updates") {
    val n = newFunction(10, 50)
    assert(n.update(0, 1.0) === List(0  -> 1.0))
    assert(n.update(2, 2.0) === List(10 -> 2.0))
    assert(n.update(4, 4.0) === List(10 -> 4.0))
    assert(n.update(8, 8.0) === List(10 -> 8.0))

    var res = n.update(12, 2.0).head
    assert(res._1 === 20)
    assertEquals(res._2, 2.0, 1e-6)

    val vs = n.update(40, 3.0)
    res = vs.head
    assert(res._1 === 40)
    assertEquals(res._2, 3.0, 1e-6)

    assert(vs.tail === Nil)
  }

}
