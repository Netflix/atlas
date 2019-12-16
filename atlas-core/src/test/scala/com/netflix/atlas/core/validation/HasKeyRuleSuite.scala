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

class HasKeyRuleSuite extends AnyFunSuite {

  private val config = ConfigFactory.parseString("key = name")
  private val rule = HasKeyRule(config)

  test("has key") {
    assert(rule.validate(Map("name" -> "foo")) === ValidationResult.Pass)
  }

  test("missing key") {
    val res = rule.validate(Map("cluster" -> "foo"))
    assert(res.isFailure)
  }
}
