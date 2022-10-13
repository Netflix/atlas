/*
 * Copyright 2014-2022 Netflix, Inc.
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

class InterpreterSuite extends FunSuite {

  import com.netflix.atlas.core.stacklang.InterpreterSuite._

  val interpreter = new Interpreter(
    List(
      PushFoo,
      Overloaded("overloaded", "one", true),
      Overloaded("overloaded", "two", true),
      Overloaded("overloaded", "three", true),
      Overloaded("overloaded2", "one", false),
      Overloaded("overloaded2", "two", true),
      Overloaded("overloaded2", "three", true),
      Overloaded("no-match", "one", false),
      Unstable
    )
  )

  def context(vs: List[Any]): Context = {
    Context(interpreter, vs, Map.empty)
  }

  test("empty") {
    assertEquals(interpreter.execute(Nil), context(Nil))
  }

  test("push items") {
    assertEquals(interpreter.execute(List("foo", "bar")), context(List("bar", "foo")))
  }

  test("execute word") {
    assertEquals(interpreter.execute(List(":push-foo")), context(List("foo")))
  }

  test("overloaded word") {
    assertEquals(interpreter.execute(List(":overloaded")), context(List("one")))
  }

  test("overloaded word and some don't match") {
    assertEquals(interpreter.execute(List(":overloaded2")), context(List("two")))
  }

  test("word with no matches") {
    val e = intercept[IllegalStateException] {
      interpreter.execute(List(":no-match"))
    }
    val expected = "no matches for word ':no-match' with stack [], candidates: [exception]"
    assertEquals(e.getMessage, expected)
  }

  test("using unstable word fails by default") {
    val e = intercept[IllegalStateException] {
      interpreter.execute(List(":unstable"))
    }
    val expected = "to use :unstable enable unstable features"
    assertEquals(e.getMessage, expected)
  }

  test("unknown word") {
    val e = intercept[IllegalStateException] {
      interpreter.execute(List("foo", ":unknown"))
    }
    assertEquals(e.getMessage, "unknown word ':unknown'")
  }

  test("unmatched closing paren") {
    val e = intercept[IllegalStateException] {
      interpreter.execute(List(")"))
    }
    assertEquals(e.getMessage, "unmatched closing parenthesis")
  }

  test("unmatched closing paren 2") {
    val e = intercept[IllegalStateException] {
      interpreter.execute(List("(", ")", ")"))
    }
    assertEquals(e.getMessage, "unmatched closing parenthesis")
  }

  test("unmatched opening paren") {
    val e = intercept[IllegalStateException] {
      interpreter.execute(List("("))
    }
    assertEquals(e.getMessage, "unmatched opening parenthesis")
  }

  test("list") {
    val list = List("(", "1", ")")
    assertEquals(interpreter.execute(list), context(List(List("1"))))
  }

  test("nested list") {
    val list = List("(", "1", "(", ")", ")")
    assertEquals(interpreter.execute(list), context(List(List("1", "(", ")"))))
  }

  test("multiple lists") {
    val list = List("(", "1", ")", "(", "2", ")")
    assertEquals(interpreter.execute(list), context(List(List("2"), List("1"))))
  }

  test("debug") {
    val list = List("(", "1", ")", "(", "2", ")")
    val expected = List(
      Interpreter.Step(list, Context(interpreter, Nil, Map.empty)),
      Interpreter.Step(list.drop(3), Context(interpreter, List(List("1")), Map.empty)),
      Interpreter.Step(Nil, Context(interpreter, List(List("2"), List("1")), Map.empty))
    )
    assertEquals(interpreter.debug(list), expected)
  }

  test("toString") {
    assertEquals(interpreter.toString, "Interpreter(9 words)")
  }

  test("typeSummary: String") {
    assertEquals(Interpreter.typeSummary(List("foo")), "[String]")
  }

  test("typeSummary: Primitive") {
    // scala 2.x: List(42, 4.0, 42L) will coerce all the values to Double. explicit Any
    // annotation needed to get expected values.
    //
    // scala 3.x: List(42, 4.0, 42L) works as expected with no type annotation
    assertEquals(Interpreter.typeSummary(List[Any](42, 42.0, 42L)), "[Long,Double,Integer]")
    assertEquals(Interpreter.typeSummary(42 :: 42.0 :: 42L :: Nil), "[Long,Double,Integer]")
  }

  test("typeSummary: List.empty") {
    assertEquals(Interpreter.typeSummary(List(List.empty)), "[List]")
  }

  test("typeSummary: List(a, b)") {
    assertEquals(Interpreter.typeSummary(List(List("a", "b"))), "[List]")
  }

  test("typeSummary: a :: b :: Nil") {
    assertEquals(Interpreter.typeSummary(List("a" :: "b" :: Nil)), "[List]")
  }

  //
  // splitAndTrim
  //

  test("splitAndTrim should split by comma") {
    val actual = Interpreter.splitAndTrim("a,b,c")
    assertEquals(actual, List("a", "b", "c"))
  }

  test("splitAndTrim should trim whitespace") {
    val actual = Interpreter.splitAndTrim("  a\n,\t\nb ,c")
    assertEquals(actual, List("a", "b", "c"))
  }

  test("splitAndTrim should ignore empty strings") {
    val actual = Interpreter.splitAndTrim(", ,\t,\n\n,")
    assertEquals(actual, Nil)
  }
}

object InterpreterSuite {

  case object PushFoo extends Word {

    override def name: String = "push-foo"

    override def matches(stack: List[Any]): Boolean = true

    override def execute(context: Context): Context = context.copy(stack = "foo" :: context.stack)

    override def summary: String = ""

    override def signature: String = "* -- * foo"

    override def examples: List[String] = Nil
  }

  case class Overloaded(name: String, value: String, matches: Boolean) extends Word {

    override def matches(stack: List[Any]): Boolean = matches

    override def execute(context: Context): Context = context.copy(stack = value :: context.stack)

    override def summary: String = ""

    override def signature: String = if (matches) "* -- * v" else "exception"

    override def examples: List[String] = Nil
  }

  case object Unstable extends Word {

    override def name: String = "unstable"

    override def matches(stack: List[Any]): Boolean = true

    override def execute(context: Context): Context = {
      context.copy(stack = "unstable" :: context.stack)
    }

    override def summary: String = ""

    override def signature: String = "* -- * unstable"

    override def examples: List[String] = Nil

    override def isStable: Boolean = false
  }
}
