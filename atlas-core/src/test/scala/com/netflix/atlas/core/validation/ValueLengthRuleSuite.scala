/*
 * Copyright 2014-2022 Netflix, Inc.
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

class ValueLengthRuleSuite extends FunSuite {

  private val config = ConfigFactory.parseString("""
      |min-length = 2
      |max-length = 4
    """.stripMargin)

  private val rule = ValueLengthRule(config)

  private def validate(k: String, v: String): ValidationResult = {
    rule.validate(Map(k -> v))
  }

  test("valid") {
    assertEquals(validate("def", "ab"), ValidationResult.Pass)
    assertEquals(validate("def", "abc"), ValidationResult.Pass)
    assertEquals(validate("def", "abcd"), ValidationResult.Pass)
  }

  test("too short") {
    val res = validate("def", "a")
    assert(res.isFailure)
  }

  test("too long") {
    val res = validate("def", "abcde")
    assert(res.isFailure)
  }
}
