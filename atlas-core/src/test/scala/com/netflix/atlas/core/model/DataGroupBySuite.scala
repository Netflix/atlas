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

import org.scalatest.FunSuite

class DataGroupBySuite extends FunSuite {

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
      ts(1), ts(2), ts(3)
    )
    val rs = groupBy(input, List("name"))
    assert(rs.size === 1)

    val expected = ts(6).withTags(Map("name" -> "test")).withLabel("(name=test)")
    assert(rs(0) === expected)
  }

  test("(,not_present,),:by") {
    val input = List(
      ts(1), ts(2), ts(3)
    )
    val rs = groupBy(input, List("not_present"))
    assert(rs.size === 0)
  }

  test("(,name,not_present,),:by") {
    val input = List(
      ts(1), ts(2), ts(3)
    )
    val rs = groupBy(input, List("name", "not_present"))
    assert(rs.size === 0)
  }

  test("(,name,mode,),:by") {
    // #1417, NPE if keyString is null for subset of datapoints
    val input = List(
      ts(1), ts(2), ts(3)
    )
    val rs = groupBy(input, List("name", "mode"))
    assert(rs.size === 1)
  }

}

