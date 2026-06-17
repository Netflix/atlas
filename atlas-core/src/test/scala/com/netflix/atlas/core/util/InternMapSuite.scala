/*
 * Copyright 2014-2026 Netflix, Inc.
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

import com.netflix.spectator.api.ManualClock
import munit.FunSuite

class InternMapSuite extends FunSuite {

  test("open hash") {
    val i = new OpenHashInternMap[String](2)
    val s1 = new String("foo")
    val s2 = new String("foo")
    assert(s1 ne s2)
    assert(i.intern(s1) eq i.intern(s2))
    assertEquals(i.size, 1)
    assertEquals(i.capacity, 3)
  }

  test("open hash resize") {
    val interner = new OpenHashInternMap[String](2)
    (1 until 10000).foreach { i =>
      val s1 = i.toString
      val s2 = new String(s1)
      assert(s1 ne s2)
      assert(interner.intern(s1) eq interner.intern(s2))
      assertEquals(interner.size, i)
    }
  }

  test("open hash retain") {
    val c = new ManualClock()
    val interner = new OpenHashInternMap[String](2, c)

    val f1 = new String("foo")
    val f2 = new String("foo")
    val b1 = new String("bar")
    val b2 = new String("bar")

    interner.intern(f1)
    assert(f1 eq interner.intern(f2))

    interner.intern(b1)
    c.setWallTime(42L)
    interner.intern(b1)
    assert(b1 eq interner.intern(b2))

    interner.retain(_ > 21L)
    assert(f1 ne interner.intern(f2))
    assert(f2 eq interner.intern(f2))
    assert(b1 eq interner.intern(b2))
  }

  test("open hash get does not insert on miss") {
    val i = new OpenHashInternMap[String](16)
    assertEquals(i.get("foo".hashCode, _ == "foo", 0L), null.asInstanceOf[String])
    assertEquals(i.size, 0)
  }

  test("open hash get returns interned value on hit") {
    val i = new OpenHashInternMap[String](16)
    val s1 = new String("foo")
    assert(i.intern(s1) eq s1)
    assert(i.get("foo".hashCode, _ == "foo", 0L) eq s1)
    assertEquals(i.size, 1)
  }

  test("open hash get with non-matching predicate is a miss") {
    val i = new OpenHashInternMap[String](16)
    i.intern(new String("foo"))
    // Probe lands on the same slot by hash, but the predicate rejects it.
    assertEquals(i.get("foo".hashCode, _ == "bar", 0L), null.asInstanceOf[String])
  }

  test("open hash get refreshes recency so retain keeps the entry") {
    val c = new ManualClock()
    val interner = new OpenHashInternMap[String](16, c)
    val f1 = new String("foo")
    interner.intern(f1) // interned at time 0

    // Access only via get with a later timestamp; recency should advance.
    assert(interner.get("foo".hashCode, _ == "foo", 42L) eq f1)

    // retain dropping anything older than 21 must keep the get-refreshed entry.
    interner.retain(_ > 21L)
    assert(interner.get("foo".hashCode, _ == "foo", 42L) eq f1)
  }

  test("concurrent get probes without inserting") {
    val i = InternMap.concurrent[String](16)
    assertEquals(i.get("foo".hashCode, _ == "foo", 0L), null.asInstanceOf[String])
    assertEquals(i.size, 0)
    val s1 = new String("foo")
    assert(i.intern(s1) eq s1)
    assert(i.get("foo".hashCode, _ == "foo", 0L) eq s1)
  }

  test("concurrent") {
    val i = InternMap.concurrent[String](2)
    val s1 = new String("foo")
    val s2 = new String("foo")
    assert(s1 ne s2)
    assert(i.intern(s1) eq i.intern(s2))
    assertEquals(i.size, 1)
  }

  test("concurrent resize") {
    val interner = InternMap.concurrent[String](2)
    (1 until 10000).foreach { i =>
      val s1 = i.toString
      val s2 = new String(s1)
      assert(s1 ne s2)
      assert(interner.intern(s1) eq interner.intern(s2))
      assertEquals(interner.size, i)
    }
  }

  test("concurrent retain") {
    val c = new ManualClock()
    val interner = InternMap.concurrent[String](2, c)

    val f1 = new String("foo")
    val f2 = new String("foo")
    val b1 = new String("bar")
    val b2 = new String("bar")

    interner.intern(f1)
    assert(f1 eq interner.intern(f2))

    interner.intern(b1)
    c.setWallTime(42L)
    interner.intern(b1)
    assert(b1 eq interner.intern(b2))

    interner.retain(_ > 21L)
    assert(f1 ne interner.intern(f2))
    assert(f2 eq interner.intern(f2))
    assert(b1 eq interner.intern(b2))
  }
}
