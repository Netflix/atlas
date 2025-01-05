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
package com.netflix.atlas.core.stacklang

import munit.FunSuite

class VocabularySuite extends FunSuite {

  import com.netflix.atlas.core.stacklang.VocabularySuite.*

  test("toMarkdown") {
    val expected =
      """
        |## call
        |
        |**Signature:** `? List -- ?`
        |
        |Pop a list off the stack and execute it as a program.
        |
        |**Examples**
        |
        |```
        |Expr: (,a,),:call
        |
        | In:
        |    1. List(a)
        |Out:
        |    1. a
        |```
        |
        |## dup
        |
        |**Signature:** `a -- a a`
        |
        |Duplicate the item on the top of the stack.
        |
        |**Examples**
        |
        |```
        |Expr: a,:dup
        |
        | In:
        |    1. a
        |Out:
        |    2. a
        |    1. a
        |```
        |
        |```
        |Expr: a,b,:dup
        |
        | In:
        |    2. a
        |    1. b
        |Out:
        |    3. a
        |    2. b
        |    1. b
        |```
        |
        |```
        |Expr: ,:dup
        |
        | In:
        |
        |Out:
        |IllegalStateException: no matches for word ':dup' with stack [], candidates: [a -- a a]
        |```
      """.stripMargin.trim
    assertEquals(TestVocabulary.toMarkdown, expected)
  }

  test("toMarkdown standard") {
    // Make sure no exceptions are thrown
    StandardVocabulary.toMarkdown
  }
}

object VocabularySuite {

  object TestVocabulary extends Vocabulary {

    def name: String = "test"

    def dependsOn: List[Vocabulary] = Nil

    def words: List[Word] = List(
      StandardVocabulary.Call,
      StandardVocabulary.Dup
    )
  }
}
