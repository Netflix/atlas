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
package com.netflix.atlas.core.stacklang

import scala.collection.immutable.ArraySeq

import com.netflix.atlas.core.stacklang.ast.DataType
import com.netflix.atlas.core.stacklang.ast.IsNumber
import com.netflix.atlas.core.stacklang.ast.Parameter
import munit.FunSuite

class TypedWordSuite extends FunSuite {

  //
  // DataType extraction
  //

  test("AnyType: extracts anything") {
    assertEquals(DataType.AnyType.extract("hello"), Some("hello"))
    assertEquals(DataType.AnyType.extract(42), Some(42))
    assertEquals(DataType.AnyType.extract(List(1, 2)), Some(List(1, 2)))
  }

  test("IntType: extracts Int from Int") {
    assertEquals(DataType.IntType.extract(42), Some(42))
  }

  test("IntType: extracts Int from String") {
    assertEquals(DataType.IntType.extract("42"), Some(42))
  }

  test("IntType: coerces Double to Int") {
    assertEquals(DataType.IntType.extract(3.14), Some(3))
  }

  test("IntType: extracts Int from Number") {
    val n: Number = java.lang.Long.valueOf(7L)
    assertEquals(DataType.IntType.extract(n), Some(7))
  }

  test("IntType: extracts Int from IsNumber") {
    val n = new IsNumber { def toNumber: Number = 42.0 }
    assertEquals(DataType.IntType.extract(n), Some(42))
  }

  test("IntType: returns None for non-Int") {
    assertEquals(DataType.IntType.extract("abc"), None)
  }

  test("DoubleType: extracts Double from Double") {
    assertEquals(DataType.DoubleType.extract(3.14), Some(3.14))
  }

  test("DoubleType: extracts Double from String") {
    assertEquals(DataType.DoubleType.extract("3.14"), Some(3.14))
  }

  test("DoubleType: extracts Double from Number") {
    val n: Number = java.lang.Long.valueOf(7L)
    assertEquals(DataType.DoubleType.extract(n), Some(7.0))
  }

  test("DoubleType: extracts Double from IsNumber") {
    val n = new IsNumber { def toNumber: Number = 3.14 }
    assertEquals(DataType.DoubleType.extract(n), Some(3.14))
  }

  test("DoubleType: returns None for non-Double") {
    assertEquals(DataType.DoubleType.extract("abc"), None)
  }

  test("StringType: extracts String") {
    assertEquals(DataType.StringType.extract("hello"), Some("hello"))
  }

  test("StringType: returns None for non-String") {
    assertEquals(DataType.StringType.extract(42), None)
  }

  //
  // TypedWord: matches
  //

  private val swapWord = new TypedWord {

    def name: String = "swap"
    def summary: String = "Swap the top two items"
    def examples: List[String] = List("a,b,:swap")

    override def parameters: ArraySeq[Parameter] = ArraySeq(
      Parameter("a", "first item", DataType.AnyType),
      Parameter("b", "second item", DataType.AnyType)
    )
    override def outputs: ArraySeq[DataType] = ArraySeq(DataType.AnyType)

    def execute(context: Context, params: IndexedSeq[Any]): Context = {
      context.copy(stack = params(0) :: params(1) :: context.stack)
    }
  }

  private val addIntsWord = new TypedWord {

    def name: String = "add-ints"
    def summary: String = "Add two integers"
    def examples: List[String] = List("1,2,:add-ints")

    override def parameters: ArraySeq[Parameter] = ArraySeq(
      Parameter("a", "first operand", DataType.IntType),
      Parameter("b", "second operand", DataType.IntType)
    )
    override def outputs: ArraySeq[DataType] = ArraySeq(DataType.IntType)

    def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val a = params(0).asInstanceOf[Int]
      val b = params(1).asInstanceOf[Int]
      context.copy(stack = (a + b) :: context.stack)
    }
  }

  test("matches: returns true when stack has matching types") {
    assert(swapWord.matches(List("a", "b")))
    assert(swapWord.matches(List(1, 2, 3))) // extra items ok
    assert(addIntsWord.matches(List("2", "1")))
    assert(addIntsWord.matches(List(2, 1)))
  }

  test("matches: returns false when stack too shallow") {
    assert(!swapWord.matches(List("a")))
    assert(!swapWord.matches(Nil))
    assert(!addIntsWord.matches(List(1)))
  }

  test("matches: returns false when types don't match") {
    assert(!addIntsWord.matches(List("abc", "1")))
    assert(!addIntsWord.matches(List("1", "abc")))
  }

  //
  // TypedWord: signature
  //

  test("signature: generated from named parameters") {
    assertEquals(swapWord.signature, "a:Any b:Any -- Any")
  }

  test("signature: generated with typed parameters") {
    assertEquals(addIntsWord.signature, "a:Int b:Int -- Int")
  }

  test("signature: unnamed parameter uses type only") {
    val word = new TypedWord {
      def name: String = "test"
      def summary: String = ""
      def examples: List[String] = Nil
      override def parameters: ArraySeq[Parameter] = ArraySeq(
        Parameter("", "anonymous", DataType.IntType)
      )
      override def outputs: ArraySeq[DataType] = ArraySeq.empty
      def execute(context: Context, params: IndexedSeq[Any]): Context = context
    }
    assertEquals(word.signature, "Int -- ")
  }

  test("signature: multiple outputs") {
    val word = new TypedWord {
      def name: String = "test"
      def summary: String = ""
      def examples: List[String] = Nil
      override def parameters: ArraySeq[Parameter] = ArraySeq(
        Parameter("a", "input", DataType.AnyType)
      )
      override def outputs: ArraySeq[DataType] = ArraySeq(DataType.AnyType, DataType.AnyType)
      def execute(context: Context, params: IndexedSeq[Any]): Context = {
        val a = params(0)
        context.copy(stack = a :: a :: context.stack)
      }
    }
    assertEquals(word.signature, "a:Any -- Any Any")
  }

  //
  // TypedWord: auto-extraction
  //

  test("execute: params are extracted and coerced") {
    val interpreter = Interpreter(List(addIntsWord))
    val result = interpreter.execute("3,4,:add-ints")
    assertEquals(result.stack, List(7))
  }

  test("execute: swap word works end-to-end") {
    val interpreter = Interpreter(List(swapWord))
    val result = interpreter.execute("a,b,:swap")
    assertEquals(result.stack, List("a", "b"))
  }

  test("execute: stack items consumed before calling typed execute") {
    // Verify that the context passed to execute(context, params) has items removed
    var receivedStack: List[Any] = null
    val word = new TypedWord {
      def name: String = "check"
      def summary: String = ""
      def examples: List[String] = Nil
      override def parameters: ArraySeq[Parameter] = ArraySeq(
        Parameter("a", "", DataType.IntType),
        Parameter("b", "", DataType.IntType)
      )
      override def outputs: ArraySeq[DataType] = ArraySeq.empty
      def execute(context: Context, params: IndexedSeq[Any]): Context = {
        receivedStack = context.stack
        context
      }
    }
    val interpreter = Interpreter(List(word))
    interpreter.execute("x,1,2,:check")
    assertEquals(receivedStack, List("x"))
  }
}
