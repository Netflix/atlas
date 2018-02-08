/*
 * Copyright 2014-2018 Netflix, Inc.
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
package com.netflix.atlas.eval.stream

import akka.http.scaladsl.model.Uri
import com.netflix.atlas.core.model.CustomVocabulary
import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.stacklang.Interpreter
import com.typesafe.config.Config

private[stream] class ExprInterpreter(config: Config) {

  private val interpreter = Interpreter(new CustomVocabulary(config).allWords)

  def eval(expr: String): List[StyleExpr] = {
    interpreter.execute(expr).stack.map {
      case ModelExtractors.PresentationType(t) => t
    }
  }

  def eval(uri: Uri): List[StyleExpr] = {
    val expr = uri.query().get("q").getOrElse {
      throw new IllegalArgumentException(s"missing required URI parameter `q`: $uri")
    }

    // Check that data expressions are supported. The streaming path doesn't support
    // time shifts.
    val results = eval(expr)
    results.foreach { result =>
      result.expr.dataExprs.foreach { dataExpr =>
        if (!dataExpr.offset.isZero) {
          throw new IllegalArgumentException(
            s":offset not supported for streaming evaluation [[$dataExpr]]"
          )
        }
      }
    }

    results
  }
}
