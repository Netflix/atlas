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

class InterpreterSuite extends FunSuite {

  import com.netflix.atlas.core.stacklang.InterpreterSuite.*

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
    assertEquals(interpreter.executeProgram(Nil), context(Nil))
  }

  test("push items") {
    assertEquals(interpreter.executeProgram(List("foo", "bar")), context(List("bar", "foo")))
  }

  test("execute word") {
    assertEquals(interpreter.executeProgram(List(":push-foo")), context(List("foo")))
  }

  test("overloaded word") {
    assertEquals(interpreter.executeProgram(List(":overloaded")), context(List("one")))
  }

  test("overloaded word and some don't match") {
    assertEquals(interpreter.executeProgram(List(":overloaded2")), context(List("two")))
  }

  test("word with no matches") {
    val e = intercept[IllegalStateException] {
      interpreter.executeProgram(List(":no-match"))
    }
    val expected = "no matches for word ':no-match' with stack [], candidates: [exception]"
    assertEquals(e.getMessage, expected)
  }

  test("using unstable word fails by default") {
    val e = intercept[IllegalStateException] {
      interpreter.executeProgram(List(":unstable"))
    }
    val expected = "to use :unstable enable unstable features"
    assertEquals(e.getMessage, expected)
  }

  test("unknown word") {
    val e = intercept[IllegalStateException] {
      interpreter.executeProgram(List("foo", ":unknown"))
    }
    assertEquals(e.getMessage, "unknown word ':unknown'")
  }

  test("unmatched closing paren") {
    val e = intercept[IllegalStateException] {
      interpreter.executeProgram(List(")"))
    }
    assertEquals(e.getMessage, "unmatched closing parenthesis")
  }

  test("unmatched closing paren 2") {
    val e = intercept[IllegalStateException] {
      interpreter.executeProgram(List("(", ")", ")"))
    }
    assertEquals(e.getMessage, "unmatched closing parenthesis")
  }

  test("unmatched opening paren") {
    val e = intercept[IllegalStateException] {
      interpreter.executeProgram(List("("))
    }
    assertEquals(e.getMessage, "unmatched opening parenthesis")
  }

  test("list") {
    val list = List("(", "1", ")")
    assertEquals(interpreter.executeProgram(list), context(List(List("1"))))
  }

  test("nested list") {
    val list = List("(", "1", "(", ")", ")")
    assertEquals(interpreter.executeProgram(list), context(List(List("1", "(", ")"))))
  }

  test("multiple lists") {
    val list = List("(", "1", ")", "(", "2", ")")
    assertEquals(interpreter.executeProgram(list), context(List(List("2"), List("1"))))
  }

  test("debug") {
    def createContext(stack: List[Any]): Context = {
      Context(interpreter, stack, Map.empty, features = Features.UNSTABLE)
    }
    val list = List("(", "1", ")", "(", "2", ")")
    val expected = List(
      Interpreter.Step(list, createContext(Nil)),
      Interpreter.Step(list.drop(3), createContext(List(List("1")))),
      Interpreter.Step(Nil, createContext(List(List("2"), List("1"))))
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

  test("splitAndTrim with escaped whitespace") {
    val space = Interpreter.escape(" ")
    val nl = Interpreter.escape("\n")
    val tab = Interpreter.escape("\t")
    val escaped = s"$space${space}a$nl,$nl${tab}b$space,c"
    val actual = Interpreter.splitAndTrim(escaped)
    assertEquals(actual, List(s"$space${space}a$nl", s"$nl${tab}b$space", "c"))
    assertEquals(Interpreter.toString(actual.reverse), escaped)
  }

  test("splitAndTrim with escaped comma") {
    val comma = Interpreter.escape(",")
    val escaped = s"a${comma}b${comma}c,d"
    val actual = Interpreter.splitAndTrim(escaped)
    assertEquals(actual, List(s"a${comma}b${comma}c", "d"))
    assertEquals(Interpreter.toString(actual.reverse), escaped)
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
