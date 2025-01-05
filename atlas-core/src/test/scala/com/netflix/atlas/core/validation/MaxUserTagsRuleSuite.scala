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

class MaxUserTagsRuleSuite extends FunSuite {

  private val config = ConfigFactory.parseString("limit = 2")
  private val rule = MaxUserTagsRule(config)

  test("ok") {
    val t1 = Map("name" -> "foo")
    val t2 = t1 + ("foo"       -> "bar")
    val t3 = t2 + ("nf.region" -> "west")
    assertEquals(rule.validate(t1), ValidationResult.Pass)
    assertEquals(rule.validate(t2), ValidationResult.Pass)
    assertEquals(rule.validate(t3), ValidationResult.Pass)
  }

  test("too many") {
    val res = rule.validate(Map("name" -> "foo", "foo" -> "bar", "abc" -> "def"))
    assert(res.isFailure)
  }

  test("id: ok") {
    val id1 = Id.create("foo")
    val id2 = id1.withTag("foo", "bar")
    val id3 = id2.withTag("nf.region", "west")
    assertEquals(rule.validate(id1), ValidationResult.Pass)
    assertEquals(rule.validate(id2), ValidationResult.Pass)
    assertEquals(rule.validate(id3), ValidationResult.Pass)
  }

  test("id: too many") {
    val id = Id.create("foo").withTags("foo", "bar", "abc", "def")
    val res = rule.validate(id)
    assert(res.isFailure)
  }
}
