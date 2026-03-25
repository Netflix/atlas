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

import com.netflix.atlas.core.stacklang.ast.DataType
import com.netflix.atlas.core.stacklang.ast.Parameter

/**
  * A macro with structured parameter and output type declarations. Extends
  * [[TypedWord]] so the LSP sees proper types, signatures, and stack matching.
  * Execution runs the body program against the original stack (parameters are
  * put back before running the body).
  *
  * @param name
  *     Operation name.
  * @param body
  *     Program tokens that will be executed.
  * @param parameters
  *     Declared input types in user-facing order (deepest stack item first).
  * @param outputs
  *     Output types produced by this word.
  * @param examples
  *     Example stacks that can be used as input.
  */
case class TypedMacro(
  name: String,
  body: List[Any],
  parameters: IndexedSeq[Parameter],
  outputs: IndexedSeq[DataType],
  summary: String,
  examples: List[String] = Nil
) extends TypedWord {

  // Pass through raw stack values without coercion — the body must see the
  // original values (e.g. a raw Query, not DataExpr.Sum(query)).
  override protected def extractParam(param: Parameter, value: Any): Any = value

  // Also accept StyleExpr where TimeSeriesExprType is expected, since body
  // words handle StyleExpr via StylePassthrough.
  override def matches(stack: List[Any]): Boolean =
    matchesWithStylePassthrough(stack)

  override def execute(context: Context, params: IndexedSeq[Any]): Context = {
    // Reconstruct the original stack and run the body against it. TypedWord
    // already popped the parameters, so push them back in stack order
    // (reverse of user-facing order).
    val restored = params.foldLeft(context.stack) { (s, v) => v :: s }
    context.interpreter.executeProgram(body, context.copy(stack = restored), unfreeze = false)
  }
}
