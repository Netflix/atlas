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

  test("retain (backward-shift) matches rehash and keeps survivors reachable") {
    val rnd = new scala.util.Random(20260617L)
    var trial = 0
    while (trial < 300) {
      val size = rnd.nextInt(2000) + 1
      // Small initial capacity forces resizes, high load factors, long collision chains, and
      // wrap-around clusters — the cases where backward-shift is easy to get wrong.
      val initCap = rnd.nextInt(64) + 2
      val clock = new ManualClock()
      val shift = new OpenHashInternMap[String](initCap, clock)
      val rehash = new OpenHashInternMap[String](initCap, clock)
      val times = scala.collection.mutable.HashMap.empty[String, Long]
      var i = 0
      while (i < size) {
        val key = s"t$trial-k$i-${rnd.nextInt(1000000)}"
        val t = rnd.nextInt(1000).toLong + 1
        times(key) = t
        clock.setWallTime(t)
        shift.intern(key)
        rehash.intern(key)
        i += 1
      }
      // Cutoff spans below-all (keep everything) .. above-all (drop everything).
      val cutoff = rnd.nextInt(1002).toLong
      shift.retain(_ > cutoff)
      rehash.retainByRehash(_ > cutoff)

      val expected = times.collect { case (k, t) if t > cutoff => k }.toSet
      assertEquals(shift.snapshot, rehash.snapshot, s"trial $trial cutoff $cutoff: shift vs rehash")
      assertEquals(shift.snapshot, expected, s"trial $trial cutoff $cutoff: snapshot vs expected")
      assertEquals(shift.size, expected.size, s"trial $trial: size")
      // Survivors must remain reachable by probing (backward-shift must not orphan any chain).
      expected.foreach(k => assert(shift.contains(k), s"trial $trial: survivor $k unreachable"))
      // Removed keys must be gone.
      times.foreach {
        case (k, t) =>
          if (t <= cutoff) assert(!shift.contains(k), s"trial $trial: removed $k still present")
      }
      trial += 1
    }
  }

  test("interner fully usable after backward-shift retain") {
    val c = new ManualClock()
    val m = new OpenHashInternMap[String](16, c)
    c.setWallTime(100)
    val survivors = (0 until 500).map { i =>
      val s = s"keep-$i"; m.intern(s); s
    }
    c.setWallTime(1)
    val removed = (0 until 500).map { i =>
      val s = s"drop-$i"; m.intern(s); s
    }

    m.retain(_ > 50) // drops the t=1 entries, keeps the t=100 entries
    assertEquals(m.size, survivors.size)

    // Membership is exactly the survivors and they are all probe-reachable.
    survivors.foreach(s => assert(m.contains(s), s"survivor $s lost"))
    removed.foreach(s => assert(!m.contains(s), s"removed $s still present"))

    // Dedup still works for survivors (re-intern returns the canonical instance, no new entry).
    val sizeAfterRetain = m.size
    survivors.foreach(s => assert(m.intern(new String(s)) eq s, s"dedup broken for $s"))
    assertEquals(m.size, sizeAfterRetain)

    // Freed slots are reusable: new keys (and previously removed ones) insert cleanly.
    c.setWallTime(200)
    val fresh = new String("brand-new")
    assert(m.intern(fresh) eq fresh)
    assert(m.intern(new String("brand-new")) eq fresh)
    assert(m.intern(new String(removed.head)) ne survivors.head)
  }

  test("repeated intern + backward-shift retain stays consistent over churn") {
    val c = new ManualClock()
    val m = new OpenHashInternMap[String](16, c)
    val expected = scala.collection.mutable.HashMap.empty[String, Long]
    val rnd = new scala.util.Random(99)
    var cycle = 0
    while (cycle < 300) {
      val now = (cycle + 1) * 10L
      c.setWallTime(now)
      // Bounded keyspace so keys recur across cycles (exercising timestamp refresh + dedup).
      val batch = rnd.nextInt(60) + 1
      var b = 0
      while (b < batch) {
        val key = s"k${rnd.nextInt(3000)}"
        m.intern(key)
        expected(key) = now
        b += 1
      }
      val cutoff = now - 50 // sliding window: drop anything not seen in the last 5 cycles
      m.retain(_ > cutoff)
      expected.filterInPlace((_, t) => t > cutoff)

      assertEquals(m.size, expected.size, s"cycle $cycle: size")
      expected.keysIterator.foreach(k => assert(m.contains(k), s"cycle $cycle: missing $k"))
      cycle += 1
    }
  }

  test("backward-shift keeps a forced collision chain reachable") {
    // All keys hash to the same home slot, forming one long probe chain; removing interior
    // entries must leave the rest reachable (the case naive in-place removal would orphan).
    final class FixedHash(val v: Int) {
      override def hashCode(): Int = 0
      override def equals(o: Any): Boolean = o match {
        case f: FixedHash => f.v == v
        case _            => false
      }
    }
    val c = new ManualClock()
    val m = new OpenHashInternMap[FixedHash](8, c)
    val keys = (0 until 10).map { v =>
      // Even v older (will be dropped), odd v newer (kept).
      c.setWallTime(if (v % 2 == 0) 1L else 100L)
      val k = new FixedHash(v)
      m.intern(k)
      k
    }
    m.retain(_ > 50L)
    keys.foreach { k =>
      if (k.v % 2 == 0) assert(!m.contains(k), s"dropped ${k.v} still present")
      else assert(m.contains(k), s"kept ${k.v} unreachable")
    }
    assertEquals(m.size, 5)
  }

  test("backward-shift handles a collision chain that wraps the table end") {
    final class FixedHash(val v: Int, h: Int) {
      override def hashCode(): Int = h
      override def equals(o: Any): Boolean = o match {
        case f: FixedHash => f.v == v
        case _            => false
      }
    }
    val c = new ManualClock()
    val m = new OpenHashInternMap[FixedHash](8, c)
    val len = m.capacity
    // Pick a hashCode whose home slot is the last slot, so a collision chain wraps to slot 0.
    var hc = 0
    while (Hash.reduce(Hash.lowbias32(hc), len) != len - 1) hc += 1
    // v0 lands at len-1, v1 at 0, v2 at 1, v3 at 2 (a wrapped cluster). Expire only v0 so its
    // removal backward-shifts the wrapped tail, exercising the gap > j (wrapped) range check.
    val keys = (0 until 4).map { v =>
      c.setWallTime(if (v == 0) 1L else 100L)
      val k = new FixedHash(v, hc)
      m.intern(k)
      k
    }
    require(m.capacity == len, "unexpected resize; lower the entry count")
    m.retain(_ > 50L)
    assert(!m.contains(keys(0)), "dropped wrapped-head still present")
    (1 until 4).foreach(v => assert(m.contains(keys(v)), s"kept v$v unreachable after wrap shift"))
    assertEquals(m.size, 3)
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
