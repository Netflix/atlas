/*
 * Copyright 2014-2017 Netflix, Inc.
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


class ValidCharactersRuleSuite extends FunSuite {

  private val config = ConfigFactory.parseString("")

  private val alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._"

  test("valid") {
    val rule = new ValidCharactersRule(config)
    assert(rule.validate(alpha, alpha) === ValidationResult.Pass)
  }

  test("invalid key") {
    val rule = new ValidCharactersRule(config)
    val res = rule.validate("spaces not allowed", alpha)
    assert(res.isFailure)
  }

  test("invalid value") {
    val rule = new ValidCharactersRule(config)
    val res = rule.validate(alpha, "spaces not allowed")
    assert(res.isFailure)
  }
}
