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
package com.netflix.atlas.core.validation

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class NameValueLengthRuleSuite extends AnyFunSuite {

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

  test("name valid") {
    assert(rule.validate("name", "abc") === ValidationResult.Pass)
    assert(rule.validate("name", "abcd") === ValidationResult.Pass)
    assert(rule.validate("name", "abcde") === ValidationResult.Pass)
  }

  test("name too short") {
    val res = rule.validate("name", "ab")
    assert(res.isFailure)
  }

  test("name too long") {
    val res = rule.validate("name", "abcdef")
    assert(res.isFailure)
  }

  test("others valid") {
    assert(rule.validate("def", "ab") === ValidationResult.Pass)
    assert(rule.validate("def", "abc") === ValidationResult.Pass)
    assert(rule.validate("def", "abcd") === ValidationResult.Pass)
  }

  test("others too short") {
    val res = rule.validate("def", "a")
    assert(res.isFailure)
  }

  test("others too long") {
    val res = rule.validate("def", "abcde")
    assert(res.isFailure)
  }
}
