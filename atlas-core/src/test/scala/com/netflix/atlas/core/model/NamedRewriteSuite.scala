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

import java.time.Duration
import com.netflix.atlas.core.stacklang.Interpreter
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class NamedRewriteSuite extends FunSuite {

  private val config = ConfigFactory.parseString("""
      |atlas.core.vocabulary {
      |  words = []
      |custom-averages = [
      |  {
      |    name = "node-avg"
      |    base-query = "name,numNodes,:eq"
      |    keys = ["app"]
      |  }
      |]
      |}
    """.stripMargin)

  private val interpreter = Interpreter(new CustomVocabulary(config).allWords)

  private def rawEval(program: String): List[StyleExpr] = {
    interpreter.execute(program).stack.flatMap {
      case ModelExtractors.PresentationType(t) => t.perOffset
      case v                                   => throw new MatchError(v)
    }
  }

  private def eval(program: String): List[StyleExpr] = {
    // Eval and re-encode to make sure display expression is consistent with the
    // original expression.
    interpreter.execute(rawEval(program).mkString(",")).stack.flatMap {
      case ModelExtractors.PresentationType(t) =>
        val expanded = t.rewrite {
          case nr: MathExpr.NamedRewrite => nr.evalExpr
        }
        expanded.asInstanceOf[StyleExpr].perOffset
      case v =>
        throw new MatchError(v)
    }
  }

  test("avg") {
    val actual = eval("name,a,:eq,:avg")
    val expected = eval("name,a,:eq,:sum,name,a,:eq,:count,:div")
    assertEquals(actual, expected)
  }

  test("avg with group by") {
    val actual = eval("name,a,:eq,:avg,(,name,),:by")
    val expected = eval("name,a,:eq,:sum,name,a,:eq,:count,:div,(,name,),:by")
    assertEquals(actual, expected)
  }

  test("dist-max") {
    val actual = eval("name,a,:eq,:dist-max")
    val expected = eval("statistic,max,:eq,name,a,:eq,:and,:max")
    assertEquals(actual, expected)
  }

  test("dist-max with group by") {
    val actual = eval("name,a,:eq,:dist-max,(,name,),:by")
    val expected = eval("statistic,max,:eq,name,a,:eq,:and,:max,(,name,),:by")
    assertEquals(actual, expected)
  }

  test("dist-max with offset") {
    val actual = eval("name,a,:eq,:dist-max,1h,:offset")
    val expected = eval("statistic,max,:eq,name,a,:eq,:and,:max,1h,:offset")
    assertEquals(actual, expected)
  }

  test("dist-avg") {
    val actual = eval("name,a,:eq,:dist-avg")
    val expected = eval(
      "statistic,(,totalTime,totalAmount,),:in,:sum,statistic,count,:eq,:sum,:div,name,a,:eq,:cq"
    )
    assertEquals(actual, expected)
  }

  test("dist-avg with group by") {
    val actual = eval("name,a,:eq,:dist-avg,(,name,),:by")
    val expected = eval(
      "statistic,(,totalTime,totalAmount,),:in,:sum,statistic,count,:eq,:sum,:div,name,a,:eq,:cq,(,name,),:by"
    )
    assertEquals(actual, expected)
  }

  test("avg, group by with offset") {
    val actual = eval("name,a,:eq,:avg,(,b,),:by,1h,:offset")
    val expected = eval("name,a,:eq,:dup,:sum,:swap,:count,:div,(,b,),:by,1h,:offset")
    assertEquals(actual, expected)
  }

  test("avg, group by, max with offset") {
    val actual = eval("name,a,:eq,:avg,(,b,),:by,:max,1h,:offset")
    val expected = eval("name,a,:eq,:dup,:sum,:swap,:count,:div,1h,:offset,(,b,),:by,:max")
    assertEquals(actual, expected)
  }

  test("node-avg, group by, max with offset") {
    val actual = eval("name,a,:eq,:node-avg,(,app,),:by,:max,1h,:offset")
    val expected = eval("name,a,:eq,name,numNodes,:eq,:div,1h,:offset,(,app,),:by,:max")
    assertEquals(actual, expected)
  }

  test("node-avg, offset maintained after query rewrite") {
    val exprs = rawEval("name,a,:eq,:node-avg,1h,:offset").map { expr =>
      expr.rewrite {
        case q: Query => Query.And(q, Query.Equal("region", "east"))
      }
    }
    val offsets = exprs
      .collect {
        case t: StyleExpr =>
          t.expr.dataExprs.map(_.offset)
      }
      .flatten
      .distinct
    assertEquals(offsets, List(Duration.ofHours(1)))
  }

  test("node-avg, group by, offset maintained after query rewrite") {
    val exprs = rawEval("name,a,:eq,:node-avg,(,app,),:by,1h,:offset").map { expr =>
      expr.rewrite {
        case q: Query => Query.And(q, Query.Equal("region", "east"))
      }
    }
    val offsets = exprs
      .collect {
        case t: StyleExpr =>
          t.expr.dataExprs.map(_.offset)
      }
      .flatten
      .distinct
    assertEquals(offsets, List(Duration.ofHours(1)))
  }

  test("percentiles, offset maintained after query rewrite") {
    val exprs = rawEval("name,a,:eq,(,99,),:percentiles,1h,:offset").map { expr =>
      expr.rewrite {
        case q: Query => Query.And(q, Query.Equal("region", "east"))
      }
    }
    val offsets = exprs
      .collect {
        case t: StyleExpr =>
          t.expr.dataExprs.map(_.offset)
      }
      .flatten
      .distinct
    assertEquals(offsets, List(Duration.ofHours(1)))
  }

  // https://github.com/Netflix/atlas/issues/809
  test("percentiles, offset maintained in toString after query rewrite") {
    val exprs = rawEval("name,a,:eq,(,99,),:percentiles,1h,:offset").map { expr =>
      expr.rewrite {
        case q: Query => Query.And(q, Query.Equal("region", "east"))
      }
    }
    val actual = exprs.mkString(",")
    val expected = "name,a,:eq,region,east,:eq,:and,(,99.0,),:percentiles,PT1H,:offset"
    assertEquals(actual, expected)
  }

  test("freeze works with named rewrite, cq") {
    val actual = eval("name,a,:eq,:freeze,name,b,:eq,:avg,:list,(,app,foo,:eq,:cq,),:each")
    val expected = eval("name,a,:eq,name,b,:eq,:avg,app,foo,:eq,:cq")
    assertEquals(actual, expected)
  }

  test("freeze works with named rewrite, cg") {
    val actual = eval("name,a,:eq,:freeze,name,b,:eq,:avg,:list,(,(,b,),:cg,),:each")
    val expected = eval("name,a,:eq,name,b,:eq,:avg,(,b,),:by")
    assertEquals(actual, expected)
  }

  test("freeze works with named rewrite, add") {
    val actual = eval("name,a,:eq,:freeze,name,b,:eq,:avg,:list,(,42,:add,),:each")
    val expected = eval("name,a,:eq,name,b,:eq,:avg,42,:add")
    assertEquals(actual, expected)
  }

  test("pct rewrite") {
    val actual = eval("name,a,:eq,(,b,),:by,:pct")
    val expected = eval("name,a,:eq,(,b,),:by,:dup,:sum,:div,100,:mul")
    assertEquals(actual, expected)
  }

  test("pct rewrite with cq") {
    val actual = eval("name,a,:eq,(,b,),:by,:pct,c,:has,:cq")
    val expected = eval("name,a,:eq,(,b,),:by,:dup,:sum,:div,100,:mul,c,:has,:cq")
    assertEquals(actual, expected)
  }

  test("pct rewrite with cg") {
    val actual = eval("name,a,:eq,(,b,),:by,:pct,c,:has,(,c,),:cg")
    val expected = eval("name,a,:eq,(,b,),:by,:dup,:sum,:div,100,:mul,c,:has,(,c,),:cg")
    assertEquals(actual, expected)
  }

  test("pct after binary op") {
    val actual = eval("name,a,:eq,(,b,),:by,10,:mul,:pct")
    val expected = eval("name,a,:eq,(,b,),:by,10,:mul,:dup,:sum,:div,100,:mul")
    assertEquals(actual, expected)
  }

  test("pct after binary op with cg") {
    val actual = eval("name,a,:eq,10,:mul,:pct,(,b,),:cg")
    val expected = eval("name,a,:eq,(,b,),:by,10,:mul,:dup,:sum,:div,100,:mul")
    assertEquals(actual, expected)
  }

  // https://github.com/Netflix/atlas/issues/791
  test("pct after binary op with cq") {
    val actual = eval("name,a,:eq,(,b,),:by,10,:mul,:pct,c,:has,:cq")
    val expected = eval("name,a,:eq,(,b,),:by,10,:mul,:dup,:sum,:div,100,:mul,c,:has,:cq")
    assertEquals(actual, expected)
  }

  test("issue-763: avg with cf-max") {
    val actual = eval("name,a,:eq,:avg,:cf-max")
    val expected = eval("name,a,:eq,:sum,:cf-max,name,a,:eq,:count,:cf-max,:div")
    assertEquals(actual, expected)
  }

  test("issue-1021: offset with des macros, af") {
    val actual = eval("name,a,:eq,:des-fast,1w,:offset,foo,bar,:eq,:cq")
    val expected = eval("name,a,:eq,foo,bar,:eq,:and,:des-fast,1w,:offset")
    assertEquals(actual, expected)
  }

  test("issue-1021: offset with des macros, math") {
    val actual = eval("name,a,:eq,:sum,:des-fast,1w,:offset,foo,bar,:eq,:cq")
    val expected = eval("name,a,:eq,foo,bar,:eq,:and,:sum,:des-fast,1w,:offset")
    assertEquals(actual, expected)
  }

  test("named rewrite, avg with grouping and cg") {
    val exprs = rawEval("name,a,:eq,:sum,(,b,),:by,:avg,:list,(,(,c,),:cg,),:each")
    val exprs2 = rawEval(exprs.mkString(","))
    val expected = rawEval("name,a,:eq,:sum,(,b,c,),:by,:avg")
    assertEquals(exprs, expected)
    assertEquals(exprs2, expected)
  }

  test("named rewrite, avg and cg") {
    val exprs = rawEval("name,a,:eq,:avg,:list,(,(,c,),:cg,),:each")
    val exprs2 = rawEval(exprs.mkString(","))
    val expected = rawEval("name,a,:eq,:avg,(,c,),:by")
    assertEquals(exprs, expected)
    assertEquals(exprs2, expected)
  }

  test("named rewrite, custom avg and cg") {
    val q = "name,foo,:eq"
    val exprs = rawEval(s"$q,(,a,b,),:by,$q,:node-avg,:lt,:sum,(,a,),:by,(,c,),:cg")
    val exprs2 = rawEval(exprs.mkString(","))
    val expected = rawEval(s"$q,(,a,b,c,),:by,$q,:node-avg,(,c,),:by,:lt,(,a,c,),:by")
    assertEquals(exprs, expected)
    assertEquals(exprs2, expected)
  }

  test("named rewrite, des and cg") {
    val q = "name,foo,:eq"
    val exprs = rawEval(s"$q,(,a,),:by,:des-fast,(,b,),:cg")
    val exprs2 = rawEval(exprs.mkString(","))
    val expected = rawEval(s"$q,(,a,b,),:by,:des-fast")
    assertEquals(exprs, expected)
    assertEquals(exprs2, expected)
  }
}
