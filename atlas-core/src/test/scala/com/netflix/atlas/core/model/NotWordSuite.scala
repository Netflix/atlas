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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.stacklang.BaseWordSuite
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.StandardVocabulary
import com.netflix.atlas.core.stacklang.Word

class NotWordSuite extends BaseWordSuite {

  def interpreter: Interpreter =
    Interpreter(QueryVocabulary.allWords ::: StandardVocabulary.allWords)

  def word: Word = QueryVocabulary.Not

  def shouldMatch: List[(String, List[Any])] = List(
    ":true"   -> List(Query.False),
    ":false"  -> List(Query.True),
    "a,b,:eq" -> List(Query.Not(Query.Equal("a", "b")))
  )

  def shouldNotMatch: List[String] = List("", "a")
}
