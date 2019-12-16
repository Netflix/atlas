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
package com.netflix.atlas.akka

import akka.actor.ActorPath
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/** Sanity check the default pattern for extracting an id from the path. */
class PathsSuite extends AnyFunSuite {

  private val mapper = Paths.createMapper(ConfigFactory.load().getConfig("atlas.akka"))

  private def path(str: String): ActorPath = ActorPath.fromString(str)

  test("contains dashes") {
    val id = mapper(path("akka://test/system/IO-TCP/$123"))
    assert("IO-TCP" === id)
  }

  test("path with child") {
    val id = mapper(path("akka://test/user/foo/$123"))
    assert("foo" === id)
  }

  test("temporary actor") {
    val id = mapper(path("akka://test/user/$123"))
    assert("uncategorized" === id)
  }

  test("stream supervisor") {
    val id = mapper(path("akka://test/user/StreamSupervisor-99961"))
    assert("StreamSupervisor-" === id)
  }
}
