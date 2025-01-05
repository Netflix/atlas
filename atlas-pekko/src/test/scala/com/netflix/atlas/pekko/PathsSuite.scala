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
package com.netflix.atlas.pekko

import org.apache.pekko.actor.ActorPath
import com.typesafe.config.ConfigFactory
import munit.FunSuite

/** Sanity check the default pattern for extracting an id from the path. */
class PathsSuite extends FunSuite {

  private val mapper = Paths.createMapper(ConfigFactory.load().getConfig("atlas.pekko"))

  private def path(str: String): ActorPath = ActorPath.fromString(str)

  test("contains dashes") {
    val id = mapper(path("pekko://test/system/IO-TCP/$123"))
    assertEquals("IO-TCP", id)
  }

  test("path with child") {
    val id = mapper(path("pekko://test/user/foo/$123"))
    assertEquals("foo", id)
  }

  test("temporary actor") {
    val id = mapper(path("pekko://test/user/$123"))
    assertEquals("uncategorized", id)
  }

  test("stream supervisor") {
    val id = mapper(path("pekko://test/user/StreamSupervisor-99961"))
    assertEquals("StreamSupervisor-", id)
  }
}
