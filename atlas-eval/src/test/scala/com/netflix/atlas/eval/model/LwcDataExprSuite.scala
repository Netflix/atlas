/*
 * Copyright 2014-2019 Netflix, Inc.
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
package com.netflix.atlas.eval.model

import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import org.scalatest.funsuite.AnyFunSuite

class LwcDataExprSuite extends AnyFunSuite {

  private def styleExpr(str: String): StyleExpr = {
    val interpreter = new Interpreter(StyleVocabulary.allWords)
    interpreter.execute(str).stack match {
      case ModelExtractors.PresentationType(v) :: Nil => v
      case _                                          => throw new IllegalArgumentException(s"invalid expr: $str")
    }
  }

  test("group by equals") {
    val exprStr = "statistic,max,:eq,name,foo,:eq,:and,:max,(,nf.asg,),:by"
    val distExprStr = "name,foo,:eq,:dist-max,(,nf.asg,),:by"
    val lwcExpr = LwcDataExpr("123", exprStr, 10L)
    assert(lwcExpr.expr === styleExpr(distExprStr).expr.dataExprs.head)
    assert(lwcExpr.expr.hashCode() === styleExpr(exprStr).expr.hashCode())
  }
}
