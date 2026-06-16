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

import munit.FunSuite

class HashSuite extends FunSuite {

  test("md5") {
    assert(Hash.md5("42").toString(10) == "215089739385482443301854222253501995174")
  }

  test("sha1") {
    assert(Hash.sha1("42").toString(10) == "838146913046966959093018715372872234545534447190")
  }

  test("sha1bytes zeroPad") {
    val expected = Strings.zeroPad(Hash.sha1("42"), 40)
    val actual = Strings.zeroPad(Hash.sha1bytes("42"), 40)
    assertEquals(actual, expected)
  }

  test("reduce range") {
    // Result must always be in [0, n) for any hash, including negative values
    // and extremes such as Integer.MIN_VALUE.
    val ns = List(1, 5, 7, 8, 97, 1597, 100000)
    val hashes = List(0, 1, -1, Int.MaxValue, Int.MinValue, 0x7FFFFFFF, 0x80000000)
    ns.foreach { n =>
      hashes.foreach { h =>
        val r = Hash.reduce(h, n)
        assert(r >= 0 && r < n, s"reduce($h, $n) = $r out of range")
      }
    }
  }

  test("reduce distribution for mixed hashes") {
    // With a good avalanche mix in front, reduce should spread keys roughly
    // evenly across the slots and never wildly overload one bucket.
    val n = 1597
    val counts = new Array[Int](n)
    val total = 100000
    var i = 0
    while (i < total) {
      counts(Hash.reduce(Hash.lowbias32(i), n)) += 1
      i += 1
    }
    val expected = total.toDouble / n
    val max = counts.max
    // No bucket should hold more than ~3x the expected load.
    assert(max < expected * 3, s"max bucket $max vs expected $expected")
    assertEquals(counts.sum, total)
  }
}
