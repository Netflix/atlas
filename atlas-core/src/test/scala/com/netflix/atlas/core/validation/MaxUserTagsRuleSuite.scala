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


class MaxUserTagsRuleSuite extends FunSuite {

  private val config = ConfigFactory.parseString("limit = 2")
  private val rule = new MaxUserTagsRule(config)

  test("ok") {
    assert(rule.validate(Map("name" -> "foo")) === ValidationResult.Pass)
    assert(rule.validate(Map("name" -> "foo", "foo" -> "bar")) === ValidationResult.Pass)
    assert(rule.validate(Map("name" -> "foo", "foo" -> "bar", "nf.region" -> "west")) === ValidationResult.Pass)
  }

  test("too many") {
    val res = rule.validate(Map("name" -> "foo", "foo" -> "bar", "abc" -> "def"))
    assert(res.isFailure)
  }
}
