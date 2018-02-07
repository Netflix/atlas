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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.stacklang.Interpreter
import org.scalatest.FunSuite

class NamedRewriteSuite extends FunSuite {

  private val interpreter = Interpreter(MathVocabulary.allWords)

  private def eval(program: String): List[TimeSeriesExpr] = {
    interpreter.execute(program).stack.map {
      case nr: MathExpr.NamedRewrite         => nr.evalExpr
      case ModelExtractors.TimeSeriesType(t) => t
    }
  }

  test("avg") {
    val actual = eval("name,a,:eq,:avg")
    val expected = eval("name,a,:eq,:sum,name,a,:eq,:count,:div")
    assert(actual === expected)
  }

  test("avg with group by") {
    val actual = eval("name,a,:eq,:avg,(,name,),:by")
    val expected = eval("name,a,:eq,:sum,name,a,:eq,:count,:div,(,name,),:by")
    assert(actual === expected)
  }

  test("dist-max") {
    val actual = eval("name,a,:eq,:dist-max")
    val expected = eval("statistic,max,:eq,name,a,:eq,:and,:max")
    assert(actual === expected)
  }

  test("dist-max with group by") {
    val actual = eval("name,a,:eq,:dist-max,(,name,),:by")
    val expected = eval("statistic,max,:eq,name,a,:eq,:and,:max,(,name,),:by")
    assert(actual === expected)
  }

  test("dist-max with offset") {
    val actual = eval("name,a,:eq,:dist-max,1h,:offset")
    val expected = eval("statistic,max,:eq,name,a,:eq,:and,:max,1h,:offset")
    assert(actual === expected)
  }

  test("dist-avg") {
    val actual = eval("name,a,:eq,:dist-avg")
    val expected = eval("statistic,(,totalTime,totalAmount,),:in,:sum,statistic,count,:eq,:sum,:div,name,a,:eq,:cq")
    assert(actual === expected)
  }

  test("dist-avg with group by") {
    val actual = eval("name,a,:eq,:dist-avg,(,name,),:by")
    val expected = eval("statistic,(,totalTime,totalAmount,),:in,:sum,statistic,count,:eq,:sum,:div,name,a,:eq,:cq,(,name,),:by")
    assert(actual === expected)
  }

  test("freeze works with named rewrite, cq") {
    val actual = eval("name,a,:eq,:freeze,name,b,:eq,:avg,:list,(,app,foo,:eq,:cq,),:each")
    val expected = eval("name,a,:eq,name,b,:eq,:avg,app,foo,:eq,:cq")
    assert(actual === expected)
  }

  test("freeze works with named rewrite, add") {
    val actual = eval("name,a,:eq,:freeze,name,b,:eq,:avg,:list,(,42,:add,),:each")
    val expected = eval("name,a,:eq,name,b,:eq,:avg,42,:add")
    assert(actual === expected)
  }
}
