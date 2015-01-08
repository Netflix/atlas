/*
 * Copyright 2015 Netflix, Inc.
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


class InterpreterSuite extends FunSuite {
  import com.netflix.atlas.core.stacklang.InterpreterSuite._

  val interpreter = new Interpreter(List(
    PushFoo,
    Overloaded("overloaded", "one", true),
    Overloaded("overloaded", "two", true),
    Overloaded("overloaded", "three", true),
    Overloaded("overloaded2", "one", false),
    Overloaded("overloaded2", "two", true),
    Overloaded("overloaded2", "three", true),
    Overloaded("no-match", "one", false)
  ))

  def context(vs: List[Any]): Context = {
    Context(interpreter, vs, Map.empty)
  }

  test("empty") {
    assert(interpreter.execute(Nil) === context(Nil))
  }

  test("push items") {
    assert(interpreter.execute(List("foo", "bar")) === context(List("bar", "foo")))
  }

  test("execute word") {
    assert(interpreter.execute(List(":push-foo")) === context(List("foo")))
  }

  test("overloaded word") {
    assert(interpreter.execute(List(":overloaded")) === context(List("one")))
  }

  test("overloaded word and some don't match") {
    assert(interpreter.execute(List(":overloaded2")) === context(List("two")))
  }

  test("word with no matches") {
    val e = intercept[IllegalStateException] {
      interpreter.execute(List(":no-match"))
    }
    val expected = "no matches for word ':no-match' with stack [], candidates: [exception]"
    assert(e.getMessage === expected)
  }

  test("unknown word") {
    val e = intercept[IllegalStateException] {
      interpreter.execute(List("foo", ":unknown"))
    }
    assert(e.getMessage === "unknown word ':unknown'")
  }

  test("unmatched closing paren") {
    val e = intercept[IllegalStateException] {
      interpreter.execute(List(")"))
    }
    assert(e.getMessage === "unmatched closing parenthesis")
  }

  test("unmatched closing paren 2") {
    val e = intercept[IllegalStateException] {
      interpreter.execute(List("(", ")", ")"))
    }
    assert(e.getMessage === "unmatched closing parenthesis")
  }

  test("unmatched opening paren") {
    val e = intercept[IllegalStateException] {
      interpreter.execute(List("("))
    }
    assert(e.getMessage === "unmatched opening parenthesis")
  }

  test("list") {
    val list = List("(", "1", ")")
    assert(interpreter.execute(list) === context(List(List("1"))))
  }

  test("nested list") {
    val list = List("(", "1", "(", ")", ")")
    assert(interpreter.execute(list) === context(List(List("1", "(", ")"))))
  }

  test("multiple lists") {
    val list = List("(", "1", ")", "(", "2", ")")
    assert(interpreter.execute(list) === context(List(List("2"), List("1"))))
  }

  test("debug") {
    val list = List("(", "1", ")", "(", "2", ")")
    val expected = List(
      Interpreter.Step(list, Context(interpreter, Nil, Map.empty)),
      Interpreter.Step(list.drop(3), Context(interpreter, List(List("1")), Map.empty)),
      Interpreter.Step(Nil, Context(interpreter, List(List("2"), List("1")), Map.empty))
    )
    assert(interpreter.debug(list) === expected)
  }

  test("toString") {
    assert(interpreter.toString === "Interpreter(8 words)")
  }

  test("typeSummary: String") {
    assert(Interpreter.typeSummary(List("foo")) === "[String]")
  }

  test("typeSummary: Primitive") {
    // Why do all of these get coerced to doubles?
    assert(Interpreter.typeSummary(List(42, 42.0, 42L)) === "[Double,Double,Double]")
    assert(Interpreter.typeSummary(42 :: 42.0 :: 42L :: Nil) === "[Long,Double,Integer]")
  }

  test("typeSummary: List.empty") {
    assert(Interpreter.typeSummary(List(List.empty)) === "[List]")
  }

  test("typeSummary: List(a, b)") {
    assert(Interpreter.typeSummary(List(List("a", "b"))) === "[List]")
  }

  test("typeSummary: a :: b :: Nil") {
    assert(Interpreter.typeSummary(List("a" :: "b" :: Nil)) === "[List]")
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
}

