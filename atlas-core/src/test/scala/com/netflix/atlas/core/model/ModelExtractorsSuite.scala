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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.stacklang.Interpreter
import org.scalatest.funsuite.AnyFunSuite

class ModelExtractorsSuite extends AnyFunSuite {

  private val words = StyleVocabulary.allWords
  private val interpreter = Interpreter(words)

  private val candidates = words.filter(!_.matches(Nil))

  def completionTest(expr: String, expected: Int): Unit = {
    test(expr) {
      val result = interpreter.execute(expr)
      assert(candidates.count(_.matches(result.stack)) === expected)
    }
  }

  completionTest("name", 8)
  completionTest("name,sps", 19)
  completionTest("name,sps,:eq", 20)
  completionTest("name,sps,:eq,app,foo,:eq", 41)
  completionTest("name,sps,:eq,app,foo,:eq,:and,(,asg,)", 10)
}
