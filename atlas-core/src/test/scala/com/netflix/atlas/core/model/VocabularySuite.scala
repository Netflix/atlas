/*
 * Copyright 2014-2026 Netflix, Inc.
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
import com.netflix.atlas.core.stacklang.TypedWord
import munit.FunSuite

class VocabularySuite extends FunSuite {

  for (vocab <- StyleVocabulary :: StyleVocabulary.dependencies; w <- vocab.words) {
    test(s"${vocab.name}: ${w.name} == self") {
      assertEquals(w, w)
    }
  }

  // Words where the stack effect depends on the contents of the stack or a count argument,
  // so they cannot be described with a fixed set of parameter types.
  private val untypedWords = Set(
    "-rot",
    "cg",
    "clear",
    "each",
    "freeze",
    "list",
    "map",
    "named-rewrite",
    "ndrop",
    "nlist",
    "pick",
    "roll",
    "rot"
  )

  test("all words are typed") {
    val untyped = StyleVocabulary.allWords
      .filterNot(_.isInstanceOf[TypedWord])
      .map(_.name)
      .filterNot(untypedWords.contains)
      .distinct
      .sorted
    assertEquals(untyped, Nil)
  }

  // Words that execute a program from the stack or a variable, so the stack effect depends
  // on the program rather than the declared outputs.
  private val variableEffectWords = Set("call", "fcall")

  private val interpreter = Interpreter(StyleVocabulary.allWords)

  for {
    w <- StyleVocabulary.allWords.collect { case tw: TypedWord => tw }
    if !variableEffectWords.contains(w.name) && w.examples.nonEmpty
  } {
    test(s"examples consistent with signature: ${w.name} [${w.signature}]") {
      w.examples.foreach(ex => CustomVocabulary.validateExample(interpreter, w, ex))
    }
  }
}
