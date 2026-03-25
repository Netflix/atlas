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
import com.netflix.atlas.core.stacklang.ast.Parameter

/**
  * A word with structured parameter declarations. The `matches`, `signature`, and
  * `execute` methods are automatically derived from the declared parameters and outputs.
  *
  * Parameters are declared in user-facing order (deepest stack item first). For example,
  * a word with signature `"a:Int b:Double -- String"` would declare:
  * {{{
  *   def parameters = ArraySeq(
  *     Parameter("a", "first param", DataType.IntType),
  *     Parameter("b", "second param", DataType.DoubleType)
  *   )
  *   def outputs = ArraySeq(DataType.StringType)
  * }}}
  *
  * Subclasses implement `execute(context, params)` where `params` is an indexed sequence
  * of already-extracted values in user-facing order and the consumed items have been
  * removed from the context stack.
  */
trait TypedWord extends Word {

  /** Structured parameter declarations in user-facing order (deepest stack item first). */
  def parameters: IndexedSeq[Parameter]

  /** Output types produced by this word. Empty means the word does not push any results. */
  def outputs: IndexedSeq[DataType]

  override def signature: String = {
    val inputs = parameters.map { p =>
      if (p.name.isEmpty) p.dataType.name else s"${p.name}:${p.dataType.name}"
    }
    val output = outputs.map(_.name).mkString(" ")
    s"${inputs.mkString(" ")} -- $output"
  }

  override def matches(stack: List[Any]): Boolean = TypedWord.matches(parameters, stack)

  /**
    * Like the default `matches`, but also accepts `StyleExpr` in positions that declare
    * `TimeSeriesExprType`. Used by [[com.netflix.atlas.core.model.StylePassthrough]] and
    * [[com.netflix.atlas.core.stacklang.TypedMacro]] to allow body words and downstream
    * operations to handle style wrapping.
    */
  protected def matchesWithStylePassthrough(stack: List[Any]): Boolean =
    TypedWord.matchesWithStylePassthrough(parameters, stack)

  /**
    * Extract a single parameter value from the stack. Override to customize extraction
    * behavior, e.g., to unwrap wrapper types before delegating to the DataType extractor.
    *
    * @param param
    *     The parameter declaration.
    * @param value
    *     The raw value from the stack.
    * @return the extracted value.
    */
  protected def extractParam(param: Parameter, value: Any): Any = {
    param.dataType.extract(value).get
  }

  /**
    * Extracts typed parameters from the stack and delegates to
    * `execute(context, params)`. The consumed items are removed
    * from the context stack before the call. After execution, [[transformResult]] is called
    * to allow post-processing.
    */
  final override def execute(context: Context): Context = {
    val params = parameters
    val n = params.length
    val extracted = new Array[Any](n)
    var i = 0
    while (i < n) {
      // Fill in user-facing order directly to avoid a separate .reverse allocation
      extracted(n - 1 - i) = extractParam(params(n - 1 - i), context.stack(i))
      i += 1
    }
    val args = ArraySeq.unsafeWrapArray(extracted)
    val remainingStack = context.stack.drop(n)
    val result = execute(context.copy(stack = remainingStack), args)
    transformResult(context.stack.take(n), args, result)
  }

  /**
    * Post-process the result of execution. Called after
    * `execute(context, params)` with the original raw stack
    * values (top-of-stack first), the extracted parameters, and the result context.
    * Override to implement cross-cutting concerns like style passthrough. Default
    * implementation returns the result unchanged.
    *
    * @param rawStackValues
    *     The original stack values consumed by this word, in stack order
    *     (top-of-stack first).
    * @param params
    *     The extracted parameters in user-facing order (deepest first).
    * @param result
    *     The context returned by execute.
    */
  protected def transformResult(
    @scala.annotation.unused rawStackValues: List[Any],
    @scala.annotation.unused params: IndexedSeq[Any],
    result: Context
  ): Context = result

  /**
    * Execute the word with extracted parameters. The context stack has already had
    * the consumed items removed. `params` are in user-facing order (deepest first),
    * with values already coerced by their DataType extractors.
    */
  def execute(context: Context, params: IndexedSeq[Any]): Context
}

object TypedWord {

  /** Standard parameter matching: each parameter's DataType must accept the stack value. */
  def matches(params: IndexedSeq[Parameter], stack: List[Any]): Boolean = {
    if (stack.length < params.length) return false
    val n = params.length
    var i = 0
    while (i < n) {
      if (params(n - 1 - i).dataType.extract(stack(i)).isEmpty) return false
      i += 1
    }
    true
  }

  /**
    * Like `matches`, but also accepts `StyleExpr` where `TimeSeriesExprType` is expected.
    * This avoids duplicating the matching loop in StylePassthrough and TypedMacro.
    */
  def matchesWithStylePassthrough(params: IndexedSeq[Parameter], stack: List[Any]): Boolean = {
    import com.netflix.atlas.core.model.ModelDataTypes
    import com.netflix.atlas.core.model.StyleExpr
    if (stack.length < params.length) return false
    val n = params.length
    var i = 0
    while (i < n) {
      val param = params(n - 1 - i)
      val value = stack(i)
      if (param.dataType == ModelDataTypes.TimeSeriesExprType) {
        value match {
          case _: StyleExpr                                 => // ok
          case _ if param.dataType.extract(value).isDefined => // ok
          case _                                            => return false
        }
      } else if (param.dataType.extract(value).isEmpty) {
        return false
      }
      i += 1
    }
    true
  }
}
