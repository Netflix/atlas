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

import org.scalatest.funsuite.AnyFunSuite

class DataGroupBySuite extends AnyFunSuite {

  private val start = 0L
  private val step = 60000L

  private val n = 1

  def ts(v: Int): TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Gauge, start, step, Array(v.toDouble))
    val value = "value" -> v.toString
    if (v % 2 == 0)
      TimeSeries(Map("name" -> "test", "mode" -> "even", value), seq)
    else
      TimeSeries(Map("name" -> "test", value), seq)
  }

  def groupBy(input: List[TimeSeries], ks: List[String]): List[TimeSeries] = {
    val context = EvalContext(start, start + step * n, step)
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.True), ks)
    expr.eval(context, input).data
  }

  test("(,name,),:by") {
    val input = List(
      ts(1),
      ts(2),
      ts(3)
    )
    val rs = groupBy(input, List("name"))
    assert(rs.size === 1)

    val expected = ts(6).withTags(Map("name" -> "test")).withLabel("(name=test)")
    assert(rs.head === expected)
  }

  test("(,not_present,),:by") {
    val input = List(
      ts(1),
      ts(2),
      ts(3)
    )
    val rs = groupBy(input, List("not_present"))
    assert(rs.size === 0)
  }

  test("(,name,not_present,),:by") {
    val input = List(
      ts(1),
      ts(2),
      ts(3)
    )
    val rs = groupBy(input, List("name", "not_present"))
    assert(rs.size === 0)
  }

  test("(,name,mode,),:by") {
    // #1417, NPE if keyString is null for subset of datapoints
    val input = List(
      ts(1),
      ts(2),
      ts(3)
    )
    val rs = groupBy(input, List("name", "mode"))
    assert(rs.size === 1)
  }

  def binaryOp(ks1: List[String], ks2: List[String]): List[TimeSeries] = {
    val input = (0 until 10).map(ts).toList
    val context = EvalContext(start, start + step * n, step)
    val expr1 = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("mode", "even")), ks1)
    val expr2 = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "test")), ks2)
    val binaryOpExpr = MathExpr.Add(expr1, expr2)
    binaryOpExpr.eval(context, input).data
  }

  test("binary ops: both sides match") {
    val rs = binaryOp(List("value"), List("value"))
    assert(rs.size === 5) // only even values will be on both sides
  }

  test("binary ops: both sides match, different orders") {
    val rs = binaryOp(List("name", "value"), List("value", "name"))
    assert(rs.size === 5) // only even values will be on both sides
  }

  test("binary ops: mismatch") {
    val e = intercept[IllegalArgumentException] {
      binaryOp(List("value"), List("name"))
    }
    assert(e.getMessage.startsWith("both sides of binary operation"))
  }

  test("binary ops: subset") {
    val e = intercept[IllegalArgumentException] {
      binaryOp(List("name", "value"), List("name"))
    }
    assert(e.getMessage.startsWith("both sides of binary operation"))
  }
}
