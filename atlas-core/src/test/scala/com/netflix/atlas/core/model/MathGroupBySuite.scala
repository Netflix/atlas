/*
 * Copyright 2014-2021 Netflix, Inc.
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

import com.netflix.atlas.core.model.MathExpr.AggrMathExpr
import com.netflix.atlas.core.stacklang.Interpreter
import munit.FunSuite

class MathGroupBySuite extends FunSuite {

  private val start = 0L
  private val step = 60000L

  private val n = 1

  def ts(v: Int): TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Gauge, start, step, Array(v.toDouble))
    val mode = "mode"   -> (if (v % 2 == 0) "even" else "odd")
    val value = "value" -> v.toString
    TimeSeries(Map("name" -> "test", mode, value), seq)
  }

  def groupBy(
    input: List[TimeSeries],
    k1: List[String],
    k2: List[String],
    aggr: TimeSeriesExpr => AggrMathExpr
  ): List[TimeSeries] = {
    val context = EvalContext(start, start + step * n, step)
    val dataBy = DataExpr.GroupBy(DataExpr.Sum(Query.True), k1)
    val expr = MathExpr.GroupBy(aggr(dataBy), k2)
    expr.eval(context, input).data
  }

  test("(,name,),:by,(,name,),:by") {
    val input = List(
      ts(1),
      ts(2),
      ts(3)
    )
    val rs = groupBy(input, List("name"), List("name"), MathExpr.Sum)
    assertEquals(rs.size, 1)

    val expected = ts(6).withTags(Map("name" -> "test")).withLabel("(name=test)")
    assertEquals(rs.head, expected)
  }

  test("(,name,),:by,(,foo,),:by") {
    val input = List(
      ts(1),
      ts(2),
      ts(3)
    )

    val e = intercept[IllegalArgumentException] {
      groupBy(input, List("name"), List("foo"), MathExpr.Sum)
    }
    assertEquals(e.getMessage, "requirement failed: (,foo,) is not a subset of (,name,)")
  }

  test("(,name,mode,),:by,(,mode,),:by") {
    val input = List(
      ts(1),
      ts(2),
      ts(3)
    )
    val rs = groupBy(input, List("name", "mode"), List("mode"), MathExpr.Sum)
    assertEquals(rs.size, 2)

    val expected = List(
      ts(2).withTags(Map("mode" -> "even")).withLabel("(mode=even)"),
      ts(4).withTags(Map("mode" -> "odd")).withLabel("(mode=odd)")
    )
    assertEquals(rs, expected)
  }

  test("(,name,mode,value,),:by,(,name,mode,),:by,(,name,),:by") {
    val input = List(
      ts(1),
      ts(2),
      ts(3)
    )
    val context = EvalContext(start, start + step * n, step)
    val dataBy =
      DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "test")), List("name", "mode", "value"))
    val mathBy1 = MathExpr.GroupBy(MathExpr.Sum(dataBy), List("name", "mode"))
    val expr = MathExpr.GroupBy(MathExpr.Sum(mathBy1), List("name"))
    val rs = expr.eval(context, input).data
    assertEquals(rs.size, 1)

    val expected = List(
      ts(6).withTags(Map("name" -> "test")).withLabel("(name=test)")
    )
    assertEquals(rs, expected)
  }

  test("name,test,:eq,(,mode,),:by,(,mode,),:by") {
    val input = List(
      ts(1),
      ts(2),
      ts(3)
    )
    val context = EvalContext(start, start + step * n, step)
    val dataBy = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "test")), List("mode"))
    val expr = MathExpr.GroupBy(MathExpr.Sum(dataBy), List("mode"))
    val rs = expr.eval(context, input).data
    assertEquals(rs.size, 2)

    val expected = List(
      ts(2).withTags(Map("name" -> "test", "mode" -> "even")).withLabel("(mode=even)"),
      ts(4).withTags(Map("name" -> "test", "mode" -> "odd")).withLabel("(mode=odd)")
    )
    assertEquals(rs, expected)
  }

  test("(,value,mode,),:by,:count,(,mode,),:by") {
    val input = List(
      ts(1),
      ts(2),
      ts(3)
    )
    val rs = groupBy(input, List("value", "mode"), List("mode"), MathExpr.Count)
    assertEquals(rs.size, 2)

    val expected = List(
      ts(1).withTags(Map("mode" -> "even")).withLabel("(mode=even)"),
      ts(2).withTags(Map("mode" -> "odd")).withLabel("(mode=odd)")
    )
    assertEquals(rs, expected)
  }

  private val interpreter = Interpreter(MathVocabulary.allWords)

  private def eval(s: String): TimeSeriesExpr = {
    interpreter.execute(s).stack match {
      case ModelExtractors.TimeSeriesType(t) :: Nil => t
      case _                                        => throw new IllegalArgumentException(s)
    }
  }

  test("multi-level group by and rewrites") {
    val input = "name,sps,:eq,:sum,(,nf.cluster,nf.asg,),:by,:avg,(,nf.cluster,),:by"
    val expr = eval(input)
    assertEquals(expr.toString, input)
  }

  test("multi-level group by with intermediate math and rewrites") {
    val input = "name,sps,:eq,:sum,(,nf.cluster,nf.asg,),:by,:abs,:avg,(,nf.cluster,),:by"
    val expr = eval(input)
    assertEquals(expr.toString, input)
  }

  test("math group by and unary op") {
    val input = "name,sps,:eq,:sum,:abs,(,nf.cluster,),:by"
    val expr = eval(input)
    assertEquals(expr.toString, "name,sps,:eq,:sum,(,nf.cluster,),:by,:abs")
  }

  test("data group by, sum, unary op, math by") {
    val input = "name,sps,:eq,:sum,(,nf.cluster,nf.asg,),:by,:sum,:abs,(,nf.asg,),:by"
    val expr = eval(input)
    assertEquals(expr.toString, "name,sps,:eq,:sum,(,nf.asg,),:by,:abs")
  }

  test("data group by, max, unary op, math by") {
    val input = "name,sps,:eq,:sum,(,nf.cluster,nf.asg,),:by,:max,:abs,(,nf.asg,),:by"
    val expr = eval(input)
    assertEquals(
      expr.toString,
      "name,sps,:eq,:sum,(,nf.cluster,nf.asg,),:by,:max,(,nf.asg,),:by,:abs"
    )
  }

  test("data group by pct") {
    val input = "name,sps,:eq,(,nf.cluster,),:by,:pct"
    val expr = eval(input)
    assertEquals(expr.toString, "name,sps,:eq,:sum,(,nf.cluster,),:by,:pct")
  }

  test("multi-level group by pct") {
    val input = "name,sps,:eq,(,nf.cluster,nf.asg,),:by,:max,(,nf.asg,),:by,:pct"
    val expr = eval(input)
    assertEquals(
      expr.toString,
      "name,sps,:eq,:sum,(,nf.cluster,nf.asg,),:by,:max,(,nf.asg,),:by,:pct"
    )
  }

  test("avg rewrite followed by pct rewrite") {
    val input = "app,foo,:eq,:avg,:pct"
    val expr = eval(input)
    assertEquals(expr.toString, input)
  }

  test("avg rewrite grouped followed by pct rewrite") {
    val input = "app,foo,:eq,:avg,(,nf.cluster,),:by,:pct"
    val expr = eval(input)
    assertEquals(expr.toString, input)
  }

  test("issue-852: constant sum group by") {
    val input = "0,:const,:sum,(,foo,),:by"
    val expr = eval(input)
    assertEquals(expr.toString, "0.0,:const,:sum")
  }

  test("issue-852: constant group by") {
    val input = "0,:const,(,foo,),:by"
    val expr = eval(input)
    assertEquals(expr.toString, "0.0,:const")
  }

  test("cg with no group by") {
    val input = "foo,1,:eq,(,a,),:cg"
    val inputExplicit = "foo,1,:eq,(,a,),:by"
    val expr = eval(input)
    val exprExplicit = eval(inputExplicit)
    assertEquals(expr.toString, exprExplicit.toString)
  }

  test("cg with simple group by") {
    val input = "foo,1,:eq,(,a,),:by,(,b,c,),:cg"
    val inputExplicit = "foo,1,:eq,(,a,b,c,),:by"
    val expr = eval(input)
    val exprExplicit = eval(inputExplicit)
    assertEquals(expr.toString, exprExplicit.toString)
  }

  test("cg with multiple group by") {
    val input = "name,foo,:eq,(,a,b,),:by,name,bar,:eq,(,b,),:by,:div,name,baz,:eq,:mul,(,c,d,),:cg"
    val inputExplicit = "name,foo,:eq,(,a,b,c,d,),:by" +
      ",name,bar,:eq,(,b,c,d,),:by,:div,name,baz,:eq,(,c,d,),:by,:mul"

    val expr = eval(input)
    val exprExplicit = eval(inputExplicit)
    assertEquals(expr.toString, exprExplicit.toString)
  }
}
