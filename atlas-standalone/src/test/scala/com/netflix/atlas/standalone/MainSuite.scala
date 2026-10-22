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
package com.netflix.atlas.standalone

import java.nio.file.Files

import munit.FunSuite

class MainSuite extends FunSuite {

  test("start/shutdown") {
    Main.main(Array.empty)
    Main.shutdown()
  }

  test("config files are merged, later files take precedence") {
    val dir = Files.createTempDirectory("atlas-main-suite")
    try {
      val f1 = dir.resolve("a.conf")
      val f2 = dir.resolve("b.conf")
      Files.writeString(f1, "a = 1\nb = 1\nlist = [\"x\"]")
      Files.writeString(f2, "b = 2")
      val config = Main.mergeConfigFiles(Array(f1.toString, f2.toString))
      assertEquals(config.getInt("a"), 1)
      assertEquals(config.getInt("b"), 2)
      assertEquals(config.getStringList("list").size(), 1)
    } finally {
      Files.list(dir).forEach(p => Files.delete(p))
      Files.delete(dir)
    }
  }
}
