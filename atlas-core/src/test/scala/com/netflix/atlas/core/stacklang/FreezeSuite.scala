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

import com.netflix.atlas.core.util.Features
import munit.FunSuite

class FreezeSuite extends FunSuite {

  def interpreter: Interpreter = Interpreter(StandardVocabulary.allWords)

  test("basic operation") {
    val context = interpreter.execute("a,b,c,:freeze")
    assertEquals(context.stack, List("c", "b", "a"))
    assert(context.frozenStack.isEmpty)
  }

  test("frozen stack is isolated") {
    val context = interpreter.execute("a,b,c,:freeze,d,e,f,:clear")
    assertEquals(context.stack, List("c", "b", "a"))
    assert(context.frozenStack.isEmpty)
  }

  test("variables are cleared") {
    val e = intercept[NoSuchElementException] {
      interpreter.execute("foo,1,:set,:freeze,foo,:get")
    }
    assertEquals(e.getMessage, "key not found: foo")
  }

  test("original variables are preserved") {
    val vars = Map("foo" -> "original", "bar" -> "2")
    val context = interpreter.execute("foo,1,:set,:freeze,foo,:get,bar,:get", vars, Features.STABLE)
    assertEquals(context.stack, List("2", "original"))
  }

  test("multiple freeze operations") {
    val context = interpreter.execute("a,b,c,:freeze,d,e,f,:freeze,g,h,i,:freeze,j,k,l,:clear")
    assertEquals(context.stack, List("i", "h", "g", "f", "e", "d", "c", "b", "a"))
    assert(context.frozenStack.isEmpty)
  }

  test("freeze works with macros") {
    // Before macros would force unfreeze after execution
    val context = interpreter.execute("a,b,:freeze,d,e,:2over,:clear")
    assertEquals(context.stack, List("b", "a"))
    assert(context.frozenStack.isEmpty)
  }

  test("freeze works with :call") {
    val context = interpreter.execute("a,b,:freeze,d,(,:dup,),:call,:clear")
    assertEquals(context.stack, List("b", "a"))
    assert(context.frozenStack.isEmpty)
  }

  test("freeze works with :each") {
    val context = interpreter.execute("a,b,:freeze,(,d,),(,:dup,),:each,:clear")
    assertEquals(context.stack, List("b", "a"))
    assert(context.frozenStack.isEmpty)
  }

  test("freeze works with :map") {
    val context = interpreter.execute("a,b,:freeze,(,d,),(,:dup,),:map,:clear")
    assertEquals(context.stack, List("b", "a"))
    assert(context.frozenStack.isEmpty)
  }

  test("freeze works with :get/:set") {
    val context = interpreter.execute("a,b,:freeze,d,e,:set,d,:get,:clear")
    assertEquals(context.stack, List("b", "a"))
    assert(context.frozenStack.isEmpty)
  }
}
