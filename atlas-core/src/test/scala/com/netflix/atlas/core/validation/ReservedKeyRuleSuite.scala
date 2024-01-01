/*
 * Copyright 2014-2024 Netflix, Inc.
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
package com.netflix.atlas.core.validation

import com.typesafe.config.ConfigFactory
import munit.FunSuite

class ReservedKeyRuleSuite extends FunSuite {

  private val config = ConfigFactory.parseString("""
      |prefix = "nf."
      |allowed-keys = ["region", "job", "task"]
    """.stripMargin)

  private val rule = ReservedKeyRule(config)

  private def validate(k: String, v: String): ValidationResult = {
    rule.validate(Map(k -> v))
  }

  test("valid") {
    assertEquals(validate("nf.region", "def"), ValidationResult.Pass)
  }

  test("valid, no reserved prefix") {
    assertEquals(validate("foo", "def"), ValidationResult.Pass)
  }

  test("invalid") {
    val res = validate("nf.foo", "def")
    assert(res.isFailure)
  }

  test("job") {
    assertEquals(validate("nf.job", "def"), ValidationResult.Pass)
  }

  test("task") {
    assertEquals(validate("nf.task", "def"), ValidationResult.Pass)
  }

}
