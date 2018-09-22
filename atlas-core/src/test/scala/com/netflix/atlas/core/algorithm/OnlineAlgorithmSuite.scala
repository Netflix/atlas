/*
 * Copyright 2014-2018 Netflix, Inc.
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
package com.netflix.atlas.core.algorithm

import com.typesafe.config.ConfigException
import org.scalatest.FunSuite

class OnlineAlgorithmSuite extends FunSuite {

  test("restore unknown type") {
    val cfg = OnlineAlgorithm.toConfig(Map("type" -> "foo"))
    intercept[IllegalArgumentException] {
      OnlineAlgorithm(cfg)
    }
  }

  test("no type specified") {
    val cfg = OnlineAlgorithm.toConfig(Map("foo" -> "bar"))
    intercept[ConfigException] {
      OnlineAlgorithm(cfg)
    }
  }

  test("toConfig: string field") {
    val cfg = OnlineAlgorithm.toConfig(Map("value" -> "str"))
    assert("str" === cfg.getString("value"))
  }

  test("toConfig: int field") {
    val cfg = OnlineAlgorithm.toConfig(Map("value" -> 42))
    assert(42 === cfg.getInt("value"))
  }

  test("toConfig: double field") {
    val cfg = OnlineAlgorithm.toConfig(Map("value" -> 42.0))
    assert(42.0 === cfg.getInt("value"))
  }

  test("toConfig: double array field") {
    val cfg = OnlineAlgorithm.toConfig(Map("array" -> Array(1.0, 2.0)))
    val vs = cfg.getDoubleList("array")
    assert(vs.size() === 2)
    assert(vs.get(1) === 2.0)
  }

  test("toConfig: config field") {
    val subCfg = OnlineAlgorithm.toConfig(Map("foo" -> "bar"))
    val cfg = OnlineAlgorithm.toConfig(Map("cfg"    -> subCfg))
    assert(subCfg === cfg.getConfig("cfg"))
  }
}
