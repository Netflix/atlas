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

class ValidCharactersRuleSuite extends AnyFunSuite {

  private val config = ConfigFactory.parseString("")

  private val customPattern = ConfigFactory.parseString("""
      |default-pattern = ".a-z"
      |
      |overrides = [
      |  {
      |    key = "nf.asg"
      |    value = "-_.A-Za-z0-9^~"
      |  }
      |]
    """.stripMargin)

  private val alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._"

  test("valid") {
    val rule = ValidCharactersRule(config)
    assert(rule.validate(alpha, alpha) === ValidationResult.Pass)
  }

  test("invalid key") {
    val rule = ValidCharactersRule(config)
    val res = rule.validate("spaces not allowed", alpha)
    assert(res.isFailure)
  }

  test("invalid value") {
    val rule = ValidCharactersRule(config)
    val res = rule.validate(alpha, "spaces not allowed")
    assert(res.isFailure)
  }

  test("custom pattern valid") {
    val rule = ValidCharactersRule(customPattern)
    assert(rule.validate("abcdef", "fedcba") === ValidationResult.Pass)
  }

  test("custom pattern invalid key") {
    val rule = ValidCharactersRule(customPattern)
    val res = rule.validate(alpha, "fedcba")
    assert(res.isFailure)
  }

  test("custom pattern invalid value") {
    val rule = ValidCharactersRule(customPattern)
    val res = rule.validate("abcdef", alpha)
    assert(res.isFailure)
  }

  test("custom pattern value override") {
    val rule = ValidCharactersRule(customPattern)
    assert(rule.validate("nf.asg", alpha + "^~") === ValidationResult.Pass)
  }
}
