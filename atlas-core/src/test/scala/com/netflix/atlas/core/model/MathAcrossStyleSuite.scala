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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.stacklang.Interpreter
import munit.FunSuite

/**
  * Verify that basic math operations can be applied to StyleExprs. For binary operators
  * only one side can be a StyleExpr.
  *
  * https://github.com/Netflix/atlas/issues/761
  */
class MathAcrossStyleSuite extends FunSuite {

  import ModelExtractors.*

  private val interpreter = Interpreter(StyleVocabulary.allWords)

  private def eval(s: String): StyleExpr = {
    val stack = interpreter.execute(s).stack
    stack match {
      case PresentationType(t) :: Nil => t
      case _                          => fail(s"expected StyleExpr, found: $stack")
    }
  }

  MathVocabulary.allWords
    .filter(_.isInstanceOf[MathVocabulary.UnaryWord])
    .foreach { w =>
      test(s"${w.name}, StyleExpr op") {
        val expected = eval(s"1,:${w.name},abc,:legend")
        val actual = eval(s"1,abc,:legend,:${w.name}")
        assertEquals(actual, expected)
      }
    }

  MathVocabulary.allWords
    .filter(_.isInstanceOf[MathVocabulary.BinaryWord])
    .foreach { w =>
      val a = "a,:has,:sum"
      val b = "b,:has,:sum"

      test(s"${w.name}, TimeSeriesExpr op StyleExpr") {
        val expected = eval(s"$a,$b,:${w.name},abc,:legend")
        val actual = eval(s"$a,$b,abc,:legend,:${w.name}")
        assertEquals(actual, expected)
      }

      test(s"${w.name}, StyleExpr op TimeSeriesExpr") {
        val expected = eval(s"$a,$b,:${w.name},abc,:legend")
        val actual = eval(s"$a,abc,:legend,$b,:${w.name}")
        assertEquals(actual, expected)
      }

      test(s"${w.name}, StyleExpr op StyleExpr") {
        val expected = eval(s"$a,$b,:${w.name}")
        val actual = eval(s"$a,a,:legend,$b,b,:legend,f00,:color,:${w.name}")
        assertEquals(actual, expected)
      }
    }

  test("binary op, strip-style for LHS") {
    val expected = eval("a,:has,a,:legend,:strip-style,b,:has,b,:legend,:add")
    val actual = eval("a,:has,b,:has,:add,b,:legend")
    assertEquals(actual, expected)
  }

  test("binary op, strip-style for RHS") {
    val expected = eval("a,:has,a,:legend,b,:has,b,:legend,:strip-style,:add")
    val actual = eval("a,:has,b,:has,:add,a,:legend")
    assertEquals(actual, expected)
  }

  test("clamp-min") {
    val expected = eval("a,:has,1,:clamp-min,abc,:legend")
    val actual = eval("a,:has,abc,:legend,1,:clamp-min")
    assertEquals(actual, expected)
  }

  test("clamp-max") {
    val expected = eval("a,:has,1,:clamp-max,abc,:legend")
    val actual = eval("a,:has,abc,:legend,1,:clamp-max")
    assertEquals(actual, expected)
  }

  test("rolling-count") {
    val expected = eval("a,:has,1,:rolling-count,abc,:legend")
    val actual = eval("a,:has,abc,:legend,1,:rolling-count")
    assertEquals(actual, expected)
  }

  test("des") {
    val expected = eval("a,:has,1,0.1,0.2,:des,abc,:legend")
    val actual = eval("a,:has,abc,:legend,1,0.1,0.2,:des")
    assertEquals(actual, expected)
  }

  test("sdes") {
    val expected = eval("a,:has,1,0.1,0.2,:sdes,abc,:legend")
    val actual = eval("a,:has,abc,:legend,1,0.1,0.2,:sdes")
    assertEquals(actual, expected)
  }

  test("trend") {
    val expected = eval("a,:has,5m,:trend,abc,:legend")
    val actual = eval("a,:has,abc,:legend,5m,:trend")
    assertEquals(actual, expected)
  }

  test("integral") {
    val expected = eval("a,:has,:integral,abc,:legend")
    val actual = eval("a,:has,abc,:legend,:integral")
    assertEquals(actual, expected)
  }

  test("derivative") {
    val expected = eval("a,:has,:derivative,abc,:legend")
    val actual = eval("a,:has,abc,:legend,:derivative")
    assertEquals(actual, expected)
  }

  test("stat") {
    val expected = eval("a,:has,max,:stat,abc,:legend")
    val actual = eval("a,:has,abc,:legend,max,:stat")
    assertEquals(actual, expected)
  }

  test("filter") {
    val expected = eval("a,:has,:stat-max,1,:gt,:filter,abc,:legend")
    val actual = eval("a,:has,abc,:legend,:stat-max,1,:gt,:filter")
    assertEquals(actual, expected)
  }
}
