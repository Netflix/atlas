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

  // Probe for getOrIntern over String keys: matches by content, builds a fresh String on a miss,
  // and counts how many times create was invoked so tests can assert no allocation on a hit.
  private class StringProbe(target: String) extends InternMap.InternProbe[String] {

    var created = 0
    def matches(v: String): Boolean = v == target
    def create(): String = { created += 1; new String(target) }
  }

  // Probe over FixedHashKey so a test can force two keys onto the same home slot (equal hashCode,
  // unequal value) and exercise the collision-chain walk.
  private class FixedHashProbe(target: FixedHashKey) extends InternMap.InternProbe[FixedHashKey] {

    var created = 0
    def matches(v: FixedHashKey): Boolean = v == target
    def create(): FixedHashKey = { created += 1; target }
  }

  test("open hash getOrIntern: insert on miss, reuse on hit") {
    val i = new OpenHashInternMap[String](16)
    val p1 = new StringProbe("foo")
    val a = i.getOrIntern("foo".hashCode, p1, 0L)
    assertEquals(a, "foo")
    assertEquals(p1.created, 1)
    assertEquals(i.size, 1)

    val p2 = new StringProbe("foo")
    val b = i.getOrIntern("foo".hashCode, p2, 0L)
    assert(b eq a) // returns the existing instance
    assertEquals(p2.created, 0) // did not build a new value on a hit
    assertEquals(i.size, 1)
  }

  test("open hash getOrIntern refreshes recency for retain") {
    val c = new ManualClock()
    val i = new OpenHashInternMap[String](16, c)
    val first = i.getOrIntern("foo".hashCode, new StringProbe("foo"), 10L)
    // Access via getOrIntern with a later timestamp; recency should advance to 42.
    assert(i.getOrIntern("foo".hashCode, new StringProbe("foo"), 42L) eq first)
    i.retain(_ > 21L) // would drop the t=10 entry, but recency was refreshed to 42
    assert(i.getOrIntern("foo".hashCode, new StringProbe("foo"), 42L) eq first)
    assertEquals(i.size, 1)
  }

  test("concurrent getOrIntern: insert on miss, reuse on hit") {
    val i = InternMap.concurrent[String](16)
    val a = i.getOrIntern("foo".hashCode, new StringProbe("foo"), 0L)
    val p = new StringProbe("foo")
    val b = i.getOrIntern("foo".hashCode, p, 0L)
    assert(a eq b)
    assertEquals(p.created, 0)
    assertEquals(i.size, 1)
  }

  test("open hash getOrIntern walks a collision chain and inserts past non-matching entries") {
    val m = new OpenHashInternMap[FixedHashKey](16)
    // Same hashCode (same home slot) but unequal: `b` must be inserted past `a`, and both stay
    // reachable. This is the probe.matches(d) == false branch the real TagsProbe relies on.
    val a = new FixedHashKey(1, 42)
    val b = new FixedHashKey(2, 42)
    val pa = new FixedHashProbe(a)
    assert(m.getOrIntern(42, pa, 0L) eq a)
    assertEquals(pa.created, 1)
    val pb = new FixedHashProbe(b)
    assert(m.getOrIntern(42, pb, 0L) eq b) // collided with `a`, walked, inserted at next slot
    assertEquals(pb.created, 1)
    assertEquals(m.size, 2)
    // Both still found on a hit, without building anything new.
    val pa2 = new FixedHashProbe(a)
    assert(m.getOrIntern(42, pa2, 0L) eq a)
    assertEquals(pa2.created, 0)
    val pb2 = new FixedHashProbe(b)
    assert(m.getOrIntern(42, pb2, 0L) eq b)
    assertEquals(pb2.created, 0)
  }

  test("getOrIntern dedups against values inserted via intern, and vice versa") {
    // Production feeds one interner through both intern (internTagsShallow) and getOrIntern
    // (getOrInternTagsShallow), plus internEntry on retain/resize, so the two paths must agree.
    val m = new OpenHashInternMap[String](16)
    val foo = new String("foo")
    assert(m.intern(foo, 0L) eq foo) // inserted via intern
    val p = new StringProbe("foo")
    assert(m.getOrIntern("foo".hashCode, p, 0L) eq foo) // getOrIntern finds the intern'd instance
    assertEquals(p.created, 0)

    val bar = m.getOrIntern("bar".hashCode, new StringProbe("bar"), 0L) // inserted via getOrIntern
    assert(m.intern(new String("bar"), 0L) eq bar) // intern finds the getOrIntern'd instance
    assertEquals(m.size, 2)
  }

  test("open hash getOrIntern inserts through a resize and keeps all entries findable") {
    val m = new OpenHashInternMap[String](4)
    val n = 1000
    (0 until n).foreach { i =>
      val p = new StringProbe(i.toString)
      assertEquals(m.getOrIntern(i.toString.hashCode, p, 0L), i.toString)
      assertEquals(p.created, 1) // all distinct, so each is built and inserted
    }
    assertEquals(m.size, n)
    // After the resizes, every entry is still found on a hit with no new builds.
    (0 until n).foreach { i =>
      val p = new StringProbe(i.toString)
      assertEquals(m.getOrIntern(i.toString.hashCode, p, 0L), i.toString)
      assertEquals(p.created, 0)
    }
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
    val c = new ManualClock()
    val m = new OpenHashInternMap[FixedHashKey](8, c)
    val keys = (0 until 10).map { v =>
      // Even v older (will be dropped), odd v newer (kept).
      c.setWallTime(if (v % 2 == 0) 1L else 100L)
      val k = new FixedHashKey(v, 0)
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
    val c = new ManualClock()
    val m = new OpenHashInternMap[FixedHashKey](8, c)
    val len = m.capacity
    // Pick a hashCode whose home slot is the last slot, so a collision chain wraps to slot 0.
    var hc = 0
    while (Hash.reduce(Hash.lowbias32(hc), len) != len - 1) hc += 1
    // v0 lands at len-1, v1 at 0, v2 at 1, v3 at 2 (a wrapped cluster). Expire only v0 so its
    // removal backward-shifts the wrapped tail, exercising the gap > j (wrapped) range check.
    val keys = (0 until 4).map { v =>
      c.setWallTime(if (v == 0) 1L else 100L)
      val k = new FixedHashKey(v, hc)
      m.intern(k)
      k
    }
    require(m.capacity == len, "unexpected resize; lower the entry count")
    m.retain(_ > 50L)
    assert(!m.contains(keys(0)), "dropped wrapped-head still present")
    (1 until 4).foreach(v => assert(m.contains(keys(v)), s"kept v$v unreachable after wrap shift"))
    assertEquals(m.size, 3)
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

// Top-level (not a local class, so Scala 3 can check the type test in `equals`) key with a
// caller-supplied hashCode, used to force collision chains in the retain tests.
private final class FixedHashKey(val v: Int, h: Int) {

  override def hashCode(): Int = h

  override def equals(o: Any): Boolean = o match {
    case f: FixedHashKey => f.v == v
    case _               => false
  }
}
