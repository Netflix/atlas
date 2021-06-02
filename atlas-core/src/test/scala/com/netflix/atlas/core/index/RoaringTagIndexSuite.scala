/*
 * Copyright 2014-2021 Netflix, Inc.
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
package com.netflix.atlas.core.index

import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.TimeSeries
import org.roaringbitmap.RoaringBitmap

class RoaringTagIndexSuite extends TagIndexSuite {

  val index: TagIndex[TimeSeries] = {
    new RoaringTagIndex(TagIndexSuite.dataset.toArray, new IndexStats())
  }

  test("empty") {
    val idx = RoaringTagIndex.empty[Datapoint]
    assert(idx.size == 0)
  }

  test("hasNonEmptyIntersection: empty, empty") {
    val b1 = new RoaringBitmap()
    val b2 = new RoaringBitmap()
    assert(!RoaringTagIndex.hasNonEmptyIntersection(b1, b2))
  }

  test("hasNonEmptyIntersection: equal") {
    val b1 = new RoaringBitmap()
    b1.add(10)
    val b2 = new RoaringBitmap()
    b2.add(10)
    assert(RoaringTagIndex.hasNonEmptyIntersection(b1, b2))
  }

  test("hasNonEmptyIntersection: no match") {
    val b1 = new RoaringBitmap()
    (0 until 20 by 2).foreach(b1.add)
    val b2 = new RoaringBitmap()
    (1 until 21 by 2).foreach(b2.add)
    assert(!RoaringTagIndex.hasNonEmptyIntersection(b1, b2))
  }

  test("hasNonEmptyIntersection: last match") {
    val b1 = new RoaringBitmap()
    (0 until 22 by 2).foreach(b1.add)
    val b2 = new RoaringBitmap()
    (1 until 21 by 2).foreach(b2.add)
    b2.add(20)
    assert(RoaringTagIndex.hasNonEmptyIntersection(b1, b2))
  }
}
