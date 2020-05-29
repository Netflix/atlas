/*
 * Copyright 2014-2020 Netflix, Inc.
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

import java.nio.file.Files
import java.nio.file.Paths

import org.scalatest.funsuite.AnyFunSuite

@scala.annotation.nowarn
class StreamsSuite extends AnyFunSuite {

  test("scope with Stream") {
    val cwd = Paths.get(".")
    val cnt = Streams.scope(Files.list(cwd))(_.count())
    assert(cnt >= 0)
  }
}
