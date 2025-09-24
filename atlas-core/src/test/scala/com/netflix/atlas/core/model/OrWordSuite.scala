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

class OrWordSuite extends BaseWordSuite {

  def interpreter: Interpreter =
    Interpreter(QueryVocabulary.allWords ::: StandardVocabulary.allWords)

  def word: Word = QueryVocabulary.Or

  def shouldMatch: List[(String, List[Any])] = List(
    "a,b,:eq,:true"  -> List(Query.True),
    ":true,a,b,:eq"  -> List(Query.True),
    "a,b,:eq,:false" -> List(Query.Equal("a", "b")),
    ":false,a,b,:eq" -> List(Query.Equal("a", "b")),
    "a,b,:eq,c,:has" -> List(Query.Or(Query.Equal("a", "b"), Query.HasKey("c")))
  )

  def shouldNotMatch: List[String] = List("", "a", ":true", "a,:true")
}
