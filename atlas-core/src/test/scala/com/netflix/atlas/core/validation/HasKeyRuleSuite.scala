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

import com.netflix.atlas.core.util.SortedTagMap
import com.netflix.spectator.api.Id
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class HasKeyRuleSuite extends FunSuite {

  private val config = ConfigFactory.parseString("key = name")
  private val rule = HasKeyRule(config)

  test("has key") {
    assertEquals(rule.validate(Map("name" -> "foo")), ValidationResult.Pass)
  }

  test("missing key") {
    val res = rule.validate(Map("cluster" -> "foo"))
    assert(res.isFailure)
  }

  test("sorted: has key") {
    assertEquals(rule.validate(SortedTagMap("name" -> "foo")), ValidationResult.Pass)
  }

  test("sorted: missing key") {
    val res = rule.validate(SortedTagMap("cluster" -> "foo"))
    assert(res.isFailure)
  }

  test("id: has key name") {
    assertEquals(rule.validate(Id.create("foo")), ValidationResult.Pass)
  }

  test("id: has key other") {
    val id = Id.create("foo").withTag("a", "1")
    assertEquals(HasKeyRule("a").validate(id), ValidationResult.Pass)
  }

  test("id: missing key other") {
    val id = Id.create("foo").withTag("a", "1")
    assert(HasKeyRule("b").validate(id).isFailure)
  }
}
