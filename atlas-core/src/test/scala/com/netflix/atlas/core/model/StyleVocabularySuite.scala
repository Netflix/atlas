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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.model.ModelExtractors.PresentationType
import com.netflix.atlas.core.stacklang.Interpreter
import org.scalatest.funsuite.AnyFunSuite

class StyleVocabularySuite extends AnyFunSuite {

  val interpreter = new Interpreter(StyleVocabulary.allWords)

  def eval(s: String): StyleExpr = {
    interpreter.execute(s).stack match {
      case PresentationType(v) :: Nil => v
    }
  }

  test("no additional style") {
    val expr = eval(":true")
    val expected = StyleExpr(DataExpr.Sum(Query.True), Map.empty)
    assert(expr === expected)
  }

  test("alpha") {
    val expr = eval(":true,40,:alpha")
    val expected = StyleExpr(DataExpr.Sum(Query.True), Map("alpha" -> "40"))
    assert(expr === expected)
  }

  test("color") {
    val expr = eval(":true,f00,:color")
    val expected = StyleExpr(DataExpr.Sum(Query.True), Map("color" -> "f00"))
    assert(expr === expected)
  }

  test("alpha > color") {
    val expr = eval(":true,40,:alpha,f00,:color")
    val expected = StyleExpr(DataExpr.Sum(Query.True), Map("color" -> "f00"))
    assert(expr === expected)
  }

  test("alpha > color > alpha") {
    val expr = eval(":true,40,:alpha,f00,:color,60,:alpha")
    val expected = StyleExpr(DataExpr.Sum(Query.True), Map("color" -> "60ff0000"))
    assert(expr === expected)
  }
}
