/*
 * Copyright 2014-2017 Netflix, Inc.
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
import org.scalatest.FunSuite

class MathGroupBySuite extends FunSuite {

  private val start = 0L
  private val step = 60000L

  private val n = 1

  def ts(v: Int): TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Gauge, start, step, Array(v.toDouble))
    val mode = "mode" -> (if (v % 2 == 0) "even" else "odd")
    val value = "value" -> v.toString
    TimeSeries(Map("name" -> "test", mode, value), seq)
  }

  def groupBy(
      input: List[TimeSeries],
      k1: List[String],
      k2: List[String],
      aggr: TimeSeriesExpr => AggrMathExpr): List[TimeSeries] = {
    val context = EvalContext(start, start + step * n, step)
    val dataBy = DataExpr.GroupBy(DataExpr.Sum(Query.True), k1)
    val expr = MathExpr.GroupBy(aggr(dataBy), k2)
    expr.eval(context, input).data
  }

  test("(,name,),:by,(,name,),:by") {
    val input = List(
      ts(1), ts(2), ts(3)
    )
    val rs = groupBy(input, List("name"), List("name"), MathExpr.Sum)
    assert(rs.size === 1)

    val expected = ts(6).withTags(Map("name" -> "test")).withLabel("(name=test)")
    assert(rs(0) === expected)
  }

  test("(,name,),:by,(,foo,),:by") {
    val input = List(
      ts(1), ts(2), ts(3)
    )

    val e = intercept[IllegalArgumentException] {
      groupBy(input, List("name"), List("foo"), MathExpr.Sum)
    }
    assert(e.getMessage === "requirement failed: (,foo,) is not a subset of (,name,)")
  }

  test("(,name,mode,),:by,(,mode,),:by") {
    val input = List(
      ts(1), ts(2), ts(3)
    )
    val rs = groupBy(input, List("name", "mode"), List("mode"), MathExpr.Sum)
    assert(rs.size === 2)

    val expected = List(
      ts(2).withTags(Map("mode" -> "even")).withLabel("(mode=even)"),
      ts(4).withTags(Map("mode" -> "odd")).withLabel("(mode=odd)")
    )
    assert(rs === expected)
  }

  test("(,name,mode,value,),:by,(,name,mode,),:by,(,name,),:by") {
    val input = List(
      ts(1), ts(2), ts(3)
    )
    val context = EvalContext(start, start + step * n, step)
    val dataBy = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "test")), List("name", "mode", "value"))
    val mathBy1 = MathExpr.GroupBy(MathExpr.Sum(dataBy), List("name", "mode"))
    val expr = MathExpr.GroupBy(MathExpr.Sum(mathBy1), List("name"))
    val rs = expr.eval(context, input).data
    assert(rs.size === 1)

    val expected = List(
      ts(6).withTags(Map("name" -> "test")).withLabel("(name=test)")
    )
    assert(rs === expected)
  }

  test("name,test,:eq,(,mode,),:by,(,mode,),:by") {
    val input = List(
      ts(1), ts(2), ts(3)
    )
    val context = EvalContext(start, start + step * n, step)
    val dataBy = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "test")), List("mode"))
    val expr = MathExpr.GroupBy(MathExpr.Sum(dataBy), List("mode"))
    val rs = expr.eval(context, input).data
    assert(rs.size === 2)

    val expected = List(
      ts(2).withTags(Map("name" -> "test", "mode" -> "even")).withLabel("(mode=even)"),
      ts(4).withTags(Map("name" -> "test", "mode" -> "odd")).withLabel("(mode=odd)")
    )
    assert(rs === expected)
  }

  test("(,value,mode,),:by,:count,(,mode,),:by") {
    val input = List(
      ts(1), ts(2), ts(3)
    )
    val rs = groupBy(input, List("value", "mode"), List("mode"), MathExpr.Count)
    assert(rs.size === 2)

    val expected = List(
      ts(1).withTags(Map("mode" -> "even")).withLabel("(mode=even)"),
      ts(2).withTags(Map("mode" -> "odd")).withLabel("(mode=odd)")
    )
    assert(rs === expected)
  }
}

