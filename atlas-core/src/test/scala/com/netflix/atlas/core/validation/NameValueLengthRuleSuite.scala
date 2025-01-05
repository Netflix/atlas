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
package com.netflix.atlas.core.validation

import com.netflix.spectator.api.Id
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class NameValueLengthRuleSuite extends FunSuite {

  private val config = ConfigFactory.parseString("""
      |name {
      |  min-length = 3
      |  max-length = 5
      |}
      |others {
      |  min-length = 2
      |  max-length = 4
      |}
    """.stripMargin)

  private val rule = NameValueLengthRule(config)

  private def validate(k: String, v: String): ValidationResult = {
    rule.validate(Map(k -> v))
    rule.validate(if (k == "name") Id.create(v) else Id.create("foo").withTag(k, v))
  }

  test("name valid") {
    assertEquals(validate("name", "abc"), ValidationResult.Pass)
    assertEquals(validate("name", "abcd"), ValidationResult.Pass)
    assertEquals(validate("name", "abcde"), ValidationResult.Pass)
  }

  test("name too short") {
    val res = validate("name", "ab")
    assert(res.isFailure)
  }

  test("name too long") {
    val res = validate("name", "abcdef")
    assert(res.isFailure)
  }

  test("others valid") {
    assertEquals(validate("def", "ab"), ValidationResult.Pass)
    assertEquals(validate("def", "abc"), ValidationResult.Pass)
    assertEquals(validate("def", "abcd"), ValidationResult.Pass)
  }

  test("others too short") {
    val res = validate("def", "a")
    assert(res.isFailure)
  }

  test("others too long") {
    val res = validate("def", "abcde")
    assert(res.isFailure)
  }
}
