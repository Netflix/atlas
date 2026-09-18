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

import munit.FunSuite

class EvalBudgetSuite extends FunSuite {

  private val interpreter = Interpreter(StandardVocabulary.allWords)

  private def list(n: Int): String = (1 to n).mkString("(,", ",", ",)")

  /**
    * Nested `:each` where each level is stack neutral: the item is pushed by `:each` and
    * dropped by the body. Neither the call depth nor the stack size grows with the iteration
    * count, so before the budget nothing bounded the work.
    */
  private def nested(n: Int, levels: Int): String = {
    var body = "(,:drop,)"
    for (_ <- 1 to levels - 1) {
      body = s"(,${list(n)},$body,:each,:drop,)"
    }
    s"${list(n)},$body,:each"
  }

  /** Same shape, but the list is stored once and referenced with `:get` at every level. */
  private def compact(n: Int, levels: Int): String = {
    var body = "(,:drop,)"
    for (_ <- 1 to levels - 1) {
      body = s"(,L,:get,$body,:each,:drop,)"
    }
    s"L,${list(n)},:set,L,:get,$body,:each"
  }

  private def isBudgetError(e: IllegalStateException): Boolean = {
    e.getMessage.contains("operation limit exceeded")
  }

  test("nested each is bounded by the budget") {
    // Three levels over a 300 item list is ~27M iterations, which took ~2s before the budget.
    val e = intercept[IllegalStateException](interpreter.execute(nested(300, 3)))
    assert(isBudgetError(e), e.getMessage)
  }

  test("compact nested each is bounded by the budget") {
    // The `:set`/`:get` form costs 23 bytes per level, so the worst case fits well within the
    // 4k URI limit. Nine levels is the most the call depth allows, ~2e22 iterations.
    val expr = compact(300, 9)
    assert(expr.length < 4096, s"expression is ${expr.length} bytes")
    val e = intercept[IllegalStateException](interpreter.execute(expr))
    assert(isBudgetError(e), e.getMessage)
  }

  /** Nested `:map`, where each body drops the item and yields the inner mapped list. */
  private def nestedMap(n: Int, levels: Int): String = {
    var body = "(,)"
    for (_ <- 1 to levels - 1) {
      body = s"(,:drop,${list(n)},$body,:map,)"
    }
    s"${list(n)},$body,:map"
  }

  test("nested map is bounded by the budget") {
    val small = Interpreter(StandardVocabulary.allWords, maxOperations = 100L)
    val e = intercept[IllegalStateException](small.execute(nestedMap(8, 3)))
    assert(isBudgetError(e), e.getMessage)
  }

  test("budget is shared across nested calls rather than reset per call") {
    // A per-invocation budget would be restored by each nested `executeProgram` and would not
    // bound the product. Two levels of 30 is 900 iterations, over a budget of 100.
    val small = Interpreter(StandardVocabulary.allWords, maxOperations = 100L)
    val e = intercept[IllegalStateException](small.execute(nested(30, 2)))
    assert(isBudgetError(e), e.getMessage)
  }

  test("budget is restored for each top level execution") {
    // Exhausting the budget once must not leave a reused interpreter permanently broken.
    val small = Interpreter(StandardVocabulary.allWords, maxOperations = 100L)
    intercept[IllegalStateException](small.execute(nested(30, 2)))
    assertEquals(small.execute("a,b").stack, List("b", "a"))
    assertEquals(small.execute("a,b").stack, List("b", "a"))
  }

  test("budget is restored when a context is reused") {
    val small = Interpreter(StandardVocabulary.allWords, maxOperations = 100L)
    val context = Context(small, Nil, Map.empty)
    // The same context, and therefore the same budget object, used for several executions.
    (1 to 5).foreach { _ =>
      assertEquals(small.executeProgram(List("a", "b"), context).stack, List("b", "a"))
    }
  }

  test("legitimate looping expressions are unaffected") {
    // 100 item list with a multi-word body, far more than real expressions use.
    val expr = s"${list(100)},(,:dup,:drop,:dup,:drop,),:each"
    assertEquals(interpreter.execute(expr).stack.size, 100)
  }

  test("debug does not reset the budget on every iteration") {
    // `debug` does not go through `executeProgram`, so the nested calls made by `:each` would
    // look like top level executions and restore the budget each time.
    val small = Interpreter(StandardVocabulary.allWords, maxOperations = 100L)
    val e = intercept[IllegalStateException](small.debug(nested(30, 2)))
    assert(isBudgetError(e), e.getMessage)
  }

  test("debug reports the same call depth as before") {
    val steps = interpreter.debug("a,b")
    assert(steps.forall(_.context.callDepth == 0), steps.map(_.context.callDepth).toString)
  }

  test("syntaxTree does not reset the budget on every iteration") {
    // `syntaxTree` executes each word against a context it builds itself, which would look like
    // a top level execution and restore the budget for every iteration of a looping operator.
    // This is the path the language server runs on every edit.
    val expr = s"${list(50)},(,:drop,),:each"

    // With enough budget the `:each` runs to completion and consumes both the list and the
    // body, leaving an empty stack.
    val ample = Interpreter(StandardVocabulary.allWords)
    assertEquals(ample.syntaxTree(expr).stack, Nil)

    // With a budget too small for 50 iterations the word fails. `syntaxTree` recovers from the
    // failure rather than reporting it, so the evidence that the loop was stopped is that the
    // stack is untouched: the list and the body are still on it.
    val small = Interpreter(StandardVocabulary.allWords, maxOperations = 5L)
    assertEquals(small.syntaxTree(expr).stack.size, 2)
  }

  test("rebuilding a list inside a loop is charged to the budget") {
    // `execute` only sees the opening parenthesis of a list literal, so charging one operation
    // per list would let a loop body rebuild a large list for the price of a single operation.
    val small = Interpreter(StandardVocabulary.allWords, maxOperations = 200L)
    val body = s"(,:drop,${list(100)},:drop,)"
    val e = intercept[IllegalStateException](small.execute(s"${list(10)},$body,:each"))
    assert(isBudgetError(e), e.getMessage)
  }

  test("budget does not affect context equality") {
    val a = Context(interpreter, List("a"), Map.empty)
    val b = Context(interpreter, List("a"), Map.empty)
    assert(a.budget ne b.budget)
    assertEquals(a, b)
    assertEquals(a.hashCode, b.hashCode)
  }
}
