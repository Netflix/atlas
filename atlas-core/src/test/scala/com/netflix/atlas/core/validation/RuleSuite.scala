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

import com.typesafe.config.ConfigFactory
import munit.FunSuite

class RuleSuite extends FunSuite {

  private val config =
    ConfigFactory.parseString(
      """
      |rules = [
      |  {
      |    class = "com.netflix.atlas.core.validation.HasKeyRule"
      |    key = "name"
      |  },
      |  {
      |    class = "com.netflix.atlas.core.validation.KeyLengthRule"
      |    min-length = 2
      |    max-length = 60
      |  },
      |  {
      |    class = "com.netflix.atlas.core.validation.ValueLengthRule"
      |    min-length = 1
      |    max-length = 120
      |  },
      |  {
      |    class = "com.netflix.atlas.core.validation.ValidCharactersRule"
      |  },
      |  {
      |    class = "com.netflix.atlas.core.validation.KeyPatternRule"
      |    pattern = "^[-_.a-zA-Z0-9]+$"
      |  },
      |  {
      |    class = "com.netflix.atlas.core.validation.ValuePatternRule"
      |    pattern = "^[-_.a-zA-Z0-9]+$"
      |  },
      |  {
      |    class = "com.netflix.atlas.core.validation.MaxUserTagsRule"
      |    limit = 20
      |  },
      |  {
      |    class = "com.netflix.atlas.core.validation.ConfigConstructorTestRule"
      |  },
      |  {
      |    class = "com.netflix.atlas.core.validation.JavaTestRule"
      |  }
      |]
    """.stripMargin
    )

  test("load") {
    val rules = Rule.load(config.getConfigList("rules"))
    assertEquals(rules.size, 9)
  }

  test("load, useComposite") {
    val rules = Rule.load(config.getConfigList("rules"), true)
    assertEquals(rules.size, 3)
    assert(rules.head.isInstanceOf[CompositeTagRule])
    assertEquals(rules.head.asInstanceOf[CompositeTagRule].tagRules.size, 7)
  }

  test("validate ok") {
    val rules = Rule.load(config.getConfigList("rules"))
    val res = Rule.validate(Map("name" -> "foo", "status" -> "2xx"), rules)
    assert(res.isSuccess)
  }

  test("validate failure") {
    val rules = Rule.load(config.getConfigList("rules"))
    val res = Rule.validate(Map("name" -> "foo", "status" -> "2 xx"), rules)
    assert(res.isFailure)
  }

}
