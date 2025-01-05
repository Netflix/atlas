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
package com.netflix.atlas.webapi

import com.netflix.atlas.core.model.CustomVocabulary
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class ApiSettingsSuite extends FunSuite {

  test("graphVocabulary default") {
    val cfg = new ApiSettings(ConfigFactory.parseString("""
        |atlas {
        |  core.vocabulary {
        |    words = []
        |    custom-averages = []
        |  }
        |  webapi.graph.vocabulary=default
        |}
      """.stripMargin))
    assert(cfg.graphVocabulary.isInstanceOf[CustomVocabulary])
  }

  test("graphVocabulary class") {
    val cls = classOf[TestVocabulary].getName
    val cfg = new ApiSettings(ConfigFactory.parseString(s"atlas.webapi.graph.vocabulary=$cls"))
    assert(cfg.graphVocabulary.isInstanceOf[TestVocabulary])
  }

  test("load validation rules") {
    // Throws if there is a problem loading the rules
    ApiSettings.validationRules
  }
}

class TestVocabulary extends Vocabulary {

  override def name: String = "test"

  override def words: List[Word] = Nil

  override def dependsOn: List[Vocabulary] = Nil
}
