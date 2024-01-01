/*
 * Copyright 2014-2024 Netflix, Inc.
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

class LoopSuite extends FunSuite {

  private val interpreter = Interpreter(StandardVocabulary.allWords)

  test("infinite loop: fcall recursion") {
    val e = intercept[IllegalStateException] {
      interpreter.execute("loop,(,loop,:fcall,),:set,loop,:fcall")
    }
    assertEquals(e.getMessage, "looping detected")
  }

  test("infinite loop: call recursion") {
    val e = intercept[IllegalStateException] {
      interpreter.execute("loop,(,loop,:get,:call,),:set,loop,:get,:call")
    }
    assertEquals(e.getMessage, "looping detected")
  }

  test("infinite loop: nested") {
    val e = intercept[IllegalStateException] {
      interpreter.execute("a,(,b,:fcall,),:set,b,(,c,:fcall,),:set,c,(,a,:fcall,),:set,a,:fcall")
    }
    assertEquals(e.getMessage, "looping detected")
  }
}
