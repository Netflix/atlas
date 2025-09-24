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
package com.netflix.atlas.lwcapi

import com.netflix.spectator.api.ManualClock
import com.netflix.spectator.impl.StepLong
import munit.FunSuite

class StreamMetadataSuite extends FunSuite {

  test("toJson") {
    val clock = new ManualClock()
    val step = 60_000L
    val meta =
      StreamMetadata(
        "id",
        "addr",
        clock,
        new StepLong(0, clock, step),
        new StepLong(0, clock, step)
      )
    meta.updateReceived(100)
    meta.updateDropped(2)

    // Move to next interval to ensure it is polling last completed interval
    clock.setWallTime(step)
    val expected =
      """{"streamId":"id","remoteAddress":"addr","receivedMessages":100,"droppedMessages":2}"""
    assertEquals(meta.toJson, expected)
  }
}
