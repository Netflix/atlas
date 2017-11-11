/*
 * Copyright 2014-2017 Netflix, Inc.
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

import org.scalatest.FunSuite

class FreezeSuite extends FunSuite {

  def interpreter: Interpreter = Interpreter(StandardVocabulary.allWords)

  test("basic operation") {
    val context = interpreter.execute("a,b,c,:freeze")
    assert(context.stack === List("c", "b", "a"))
    assert(context.frozenStack.isEmpty)
  }

  test("frozen stack is isolated") {
    val context = interpreter.execute("a,b,c,:freeze,d,e,f,:clear")
    assert(context.stack === List("c", "b", "a"))
    assert(context.frozenStack.isEmpty)
  }

  test("variables are cleared") {
    val e = intercept[NoSuchElementException] {
      interpreter.execute("foo,1,:set,:freeze,foo,:get")
    }
    assert(e.getMessage === "key not found: foo")
  }

  test("multiple freeze operations") {
    val context = interpreter.execute("a,b,c,:freeze,d,e,f,:freeze,g,h,i,:freeze,j,k,l,:clear")
    assert(context.stack === List("i", "h", "g", "f", "e", "d", "c", "b", "a"))
    assert(context.frozenStack.isEmpty)
  }
}
