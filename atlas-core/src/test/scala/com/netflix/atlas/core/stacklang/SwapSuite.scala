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
package com.netflix.atlas.core.stacklang

class SwapSuite extends BaseWordSuite {

  def interpreter: Interpreter = Interpreter(StandardVocabulary.allWords)

  def word: Word = StandardVocabulary.Swap

  def shouldMatch: List[(String, List[Any])] = List(
    "a,b" -> List("a", "b"),
    "a,b,c,d,e" -> List("d", "e", "c", "b", "a")
  )

  def shouldNotMatch: List[String] = List("", "a")
}
