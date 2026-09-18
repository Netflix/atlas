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

/**
  * Budget for the total amount of work allowed for a single execution of a program.
  *
  * The call depth and stack size limits bound infinite recursion and unbounded stack growth,
  * but they do not bound the total amount of work. Looping operators like `:each` and `:map`
  * recurse into [[Interpreter]]`.executeProgram` once per item, so nesting them multiplies the
  * iteration count while the call depth only grows with the nesting depth and the stack size
  * stays flat. Nine levels of nesting, the most the call depth allows, over a 300 item list is
  * roughly 2e22 iterations from an expression that fits well within the URI length limit. That
  * is not distinguishable from an infinite loop in practice and cannot be interrupted by the
  * HTTP request timeout, which only sheds the response.
  *
  * The budget is a single counter for the whole execution rather than a per-invocation limit.
  * A per-invocation limit would be reset by each nested call and would not bound the product.
  *
  * Not thread safe. A budget belongs to one execution, which runs on a single thread. It is
  * held by [[Context]] so that it is shared by every `copy` of the context made during an
  * execution.
  *
  * All budgets compare as equal. The remaining count is scaffolding for the execution rather
  * than part of the logical value of a context, and [[Context]] is a case class whose
  * equality is relied on by callers comparing expected and actual results.
  */
final class EvalBudget(private var remaining: Long) {

  // Retained so the error message can report the limit like the other interpreter limits do.
  private var limit: Long = remaining

  /**
    * Restore the budget to `n` operations for the start of a new execution. Restricted to the
    * package so that a custom [[Word]] handed a [[Context]] cannot lift the limit for itself.
    */
  private[stacklang] def reset(n: Long): Unit = {
    remaining = n
    limit = n
  }

  /**
    * Account for a single operation, throwing if the budget for the execution is exhausted.
    * Reported as an `IllegalStateException` like the other interpreter limits so that it
    * surfaces as a user error rather than a server failure.
    */
  def consume(): Unit = {
    remaining -= 1
    if (remaining < 0) {
      throw new IllegalStateException(
        s"expression is too expensive: operation limit exceeded, limit of $limit, reduce " +
          "the amount of work performed by looping operators such as :each and :map"
      )
    }
  }

  override def equals(other: Any): Boolean = other.isInstanceOf[EvalBudget]

  override def hashCode: Int = classOf[EvalBudget].hashCode

  override def toString: String = "EvalBudget"
}
