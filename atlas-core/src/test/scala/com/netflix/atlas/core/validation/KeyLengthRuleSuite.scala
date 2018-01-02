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
package com.netflix.atlas.core.validation

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite


class KeyLengthRuleSuite extends FunSuite {

  private val config = ConfigFactory.parseString(
    """
      |min-length = 2
      |max-length = 4
    """.stripMargin)

  private val rule = new KeyLengthRule(config)

  test("valid") {
    assert(rule.validate("ab", "def") === ValidationResult.Pass)
    assert(rule.validate("abc", "def") === ValidationResult.Pass)
    assert(rule.validate("abcd", "def") === ValidationResult.Pass)
  }

  test("too short") {
    val res = rule.validate("a", "def")
    assert(res.isFailure)
  }

  test("too long") {
    val res = rule.validate("abcde", "def")
    assert(res.isFailure)
  }
}
