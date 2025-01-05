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

abstract class BaseWordSuite extends FunSuite {

  def interpreter: Interpreter

  def word: Word

  def shouldMatch: List[(String, List[Any])]

  def shouldNotMatch: List[String]

  shouldMatch.foreach {
    case (prg, after) =>
      test(s"should match: $prg") {
        assert(word.matches(interpreter.execute(prg).stack))
      }

      test(s"execute: $prg") {
        val c = interpreter.execute(prg)
        assertEquals(word.execute(c).stack, after)
      }
  }

  shouldNotMatch.foreach { prg =>
    test(s"should not match: $prg") {
      assert(!word.matches(interpreter.execute(prg).stack))
    }
  }
}
