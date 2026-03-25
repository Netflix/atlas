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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.stacklang.Context
import com.netflix.atlas.core.stacklang.TypedWord
import com.netflix.atlas.core.stacklang.ast.Parameter

/**
  * Mixin for TypedWord that adds transparent StyleExpr passthrough. When mixed in,
  * `matches` will also accept `StyleExpr` in positions that declare `TimeSeriesExprType`,
  * and `transformResult` will re-wrap the output with the style settings from the input.
  *
  * Policy:
  *  - If exactly one input was a StyleExpr, the first TimeSeriesExpr on the result
  *    stack is wrapped with that StyleExpr's settings.
  *  - If multiple inputs were StyleExpr, the style is stripped to avoid ambiguity.
  */
trait StylePassthrough extends TypedWord {

  override protected def extractParam(param: Parameter, value: Any): Any = {
    if (param.dataType == ModelDataTypes.TimeSeriesExprType) {
      value match {
        case s: StyleExpr => s.expr
        case _            => param.dataType.extract(value).get
      }
    } else {
      param.dataType.extract(value).get
    }
  }

  override def matches(stack: List[Any]): Boolean =
    matchesWithStylePassthrough(stack)

  override protected def transformResult(
    rawStackValues: List[Any],
    params: IndexedSeq[Any],
    result: Context
  ): Context = {
    // Find StyleExpr instances among the raw stack values for TimeSeriesExprType params
    val paramDefs = parameters
    val n = paramDefs.length
    var styles: List[StyleExpr] = Nil
    var i = 0
    while (i < n) {
      val param = paramDefs(n - 1 - i)
      if (param.dataType == ModelDataTypes.TimeSeriesExprType) {
        rawStackValues(i) match {
          case s: StyleExpr => styles = s :: styles
          case _            =>
        }
      }
      i += 1
    }

    styles match {
      case single :: Nil =>
        // Exactly one StyleExpr input: re-wrap the top of the result stack
        result.stack match {
          case (t: TimeSeriesExpr) :: rest =>
            result.copy(stack = StyleExpr(t, single.settings) :: rest)
          case _ => result
        }
      case _ =>
        // Zero or multiple StyleExpr inputs: return result as-is (style stripped)
        result
    }
  }
}
