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
package com.netflix.atlas.core.model

import munit.FunSuite

class BlockStatsSuite extends FunSuite {

  test("resize") {
    BlockStats.clear()
    val block = CompressedArrayBlock(0L, 60)

    BlockStats.inc(block)
    assertEquals(BlockStats.overallCount, 1)
    assertEquals(BlockStats.overallBytes, 22L)

    // Resize the block for a single value
    block.update(0, 2.0)
    assertEquals(BlockStats.overallCount, 1)
    assertEquals(BlockStats.overallBytes, 46L)

    // Resize the block for up to 4 values
    block.update(1, 3.0)
    assertEquals(BlockStats.overallCount, 1)
    assertEquals(BlockStats.overallBytes, 70L)

    // Resize the block for up to 12 values, 2 already used
    (0 until 10).foreach { i =>
      block.update(i, i + 4.0)
    }
    assertEquals(BlockStats.overallCount, 1)
    assertEquals(BlockStats.overallBytes, 134L)

    // Resize to full array
    block.update(1, 14.0)
    assertEquals(BlockStats.overallCount, 1)
    assertEquals(BlockStats.overallBytes, 486L)

    BlockStats.dec(block)
    assertEquals(BlockStats.overallCount, 0)
    assertEquals(BlockStats.overallBytes, 0L)
  }
}
