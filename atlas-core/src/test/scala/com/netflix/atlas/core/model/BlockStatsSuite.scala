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
package com.netflix.atlas.core.model

import org.scalatest.funsuite.AnyFunSuite

class BlockStatsSuite extends AnyFunSuite {

  test("resize") {
    BlockStats.clear()
    val block = CompressedArrayBlock(0L, 60)

    BlockStats.inc(block)
    assert(BlockStats.overallCount === 1)
    assert(BlockStats.overallBytes === 22)

    // Resize the block for a single value
    block.update(0, 2.0)
    assert(BlockStats.overallCount === 1)
    assert(BlockStats.overallBytes === 46)

    // Resize the block for up to 4 values
    block.update(1, 3.0)
    assert(BlockStats.overallCount === 1)
    assert(BlockStats.overallBytes === 70)

    // Resize the block for up to 12 values, 2 already used
    (0 until 10).foreach { i =>
      block.update(i, i + 4.0)
    }
    assert(BlockStats.overallCount === 1)
    assert(BlockStats.overallBytes === 134)

    // Resize to full array
    block.update(1, 14.0)
    assert(BlockStats.overallCount === 1)
    assert(BlockStats.overallBytes === 486)

    BlockStats.dec(block)
    assert(BlockStats.overallCount === 0)
    assert(BlockStats.overallBytes === 0)
  }
}
