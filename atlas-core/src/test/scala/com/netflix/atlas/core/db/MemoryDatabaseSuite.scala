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
package com.netflix.atlas.core.db

import com.netflix.atlas.core.model.*
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.ManualClock
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class MemoryDatabaseSuite extends FunSuite {

  private val interpreter = new Interpreter(DataVocabulary.allWords)

  private val step = DefaultSettings.stepSize

  private val clock = new ManualClock()
  private val registry = new DefaultRegistry(clock)

  private var db: MemoryDatabase = _

  override def beforeEach(context: BeforeEach): Unit = {
    clock.setWallTime(0L)

    db = new MemoryDatabase(
      registry,
      ConfigFactory.parseString("""
          |block-size = 60
          |num-blocks = 2
          |rebuild-frequency = 10s
          |test-mode = true
          |intern-while-building = true
        """.stripMargin)
    )

    addData("a", 1.0, 2.0, 3.0)
    addData("b", 3.0, 2.0, 1.0)

    addRollupData("c", 4.0, 5.0, 6.0)
    addRollupData("c", 5.0, 6.0, 7.0)
    addRollupData("c", 6.0, 7.0, 8.0)
  }

  private val context = EvalContext(0, 3 * step, step)

  private def addData(name: String, values: Double*): Unit = {
    val tags = Map("name" -> name)
    val id = TaggedItem.computeId(tags)
    val data = values.toList.zipWithIndex.map {
      case (v, i) =>
        clock.setWallTime(i * step)
        DatapointTuple(id, tags, i * step, v)
    }
    db.update(data)
    db.index.rebuildIndex()
  }

  private def addRollupData(name: String, values: Double*): Unit = {
    val tags = Map("name" -> name)
    val id = TaggedItem.computeId(tags)
    val data = values.toList.zipWithIndex.map {
      case (v, i) =>
        clock.setWallTime(i * step)
        DatapointTuple(id, tags, i * step, v)
    }
    data.foreach(db.rollup)
    db.index.rebuildIndex()
  }

  private def expr(str: String): DataExpr = {
    interpreter.execute(str).stack match {
      case ModelExtractors.DataExprType(v) :: Nil => v
      case _ => throw new IllegalArgumentException(s"invalid data expr: $str")
    }
  }

  private def exec(str: String, s: Long = step): List[TimeSeries] = {
    val ctxt = context.copy(step = s)
    db.execute(ctxt, expr(str)).sortWith(_.label < _.label).map { t =>
      t.mapTimeSeq(s => s.bounded(context.start, context.end))
    }
  }

  private def ts(label: String, mul: Int, values: Double*): TimeSeries = {
    TimeSeries(Map.empty, label, new ArrayTimeSeq(DsType.Rate, 0L, mul * step, values.toArray))
  }

  private def ts(name: String, label: String, mul: Int, values: Double*): TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Rate, 0L, mul * step, values.toArray)
    TimeSeries(Map("name" -> name, "foo" -> "bar"), label, seq)
  }

  private def expTS(name: String, label: String, mul: Int, values: Double*): TimeSeries = {
    val tmp = ts(name, label, mul, values*)
    tmp.withTags(tmp.tags - "foo")
  }

  test(":eq query") {
    val result = exec("name,a,:eq")
    assertEquals(result.map(_.tags), List(Map("name" -> "a")))
    assertEquals(result, List(expTS("a", "sum(name=a)", 1, 1.0, 2.0, 3.0)))
  }

  test(":in query") {
    assertEquals(exec("name,(,a,b,),:in"), List(ts("sum(name in (a,b))", 1, 4.0, 4.0, 4.0)))
  }

  test(":re query") {
    assertEquals(exec("name,[ab]$,:re"), List(ts("sum(name~/^[ab]$/)", 1, 4.0, 4.0, 4.0)))
  }

  test(":contains query") {
    assertEquals(exec("name,a,:contains"), List(ts("sum(name~/^.*a/)", 1, 1.0, 2.0, 3.0)))
  }

  test(":has query") {
    assertEquals(exec("name,:has"), List(ts("sum(has(name))", 1, 19.0, 22.0, 25.0)))
  }

  test(":offset expr") {
    assertEquals(
      exec(":true,:sum,1m,:offset"),
      List(
        ts("sum(true) (offset=1m)", 1, Double.NaN, 19.0, 22.0)
      )
    )
  }

  test(":sum expr") {
    assertEquals(exec(":true,:sum"), List(ts("sum(true)", 1, 19.0, 22.0, 25.0)))
  }

  test(":count expr") {
    assertEquals(exec(":true,:count"), List(ts("count(true)", 1, 5.0, 5.0, 5.0)))
  }

  test(":min expr") {
    assertEquals(exec(":true,:min"), List(ts("min(true)", 1, 1.0, 2.0, 1.0)))
  }

  test(":max expr") {
    assertEquals(exec(":true,:max"), List(ts("max(true)", 1, 6.0, 7.0, 8.0)))
  }

  test(":by expr") {
    val expected = List(
      expTS("a", "(name=a)", 1, 1.0, 2.0, 3.0),
      expTS("b", "(name=b)", 1, 3.0, 2.0, 1.0),
      expTS("c", "(name=c)", 1, 15.0, 18.0, 21.0)
    )
    assertEquals(exec(":true,(,name,),:by"), expected)
  }

  test(":all expr") {
    val expected = List(
      expTS("a", "name=a", 1, 1.0, 2.0, 3.0),
      expTS("b", "name=b", 1, 3.0, 2.0, 1.0),
      expTS("c", "name=c", 1, 15.0, 18.0, 21.0)
    )
    assertEquals(exec(":true,:all"), expected)
  }

  test(":sum expr, c=3") {
    assertEquals(exec(":true,:sum", 3 * step), List(ts("sum(true)", 3, 22.0)))
  }

  test(":sum expr, c=3, cf=sum") {
    assertEquals(exec(":true,:sum,:cf-sum", 3 * step), List(ts("sum(true)", 3, 66.0)))
  }

  test(":sum expr, c=3, cf=max") {
    assertEquals(exec(":true,:sum,:cf-max", 3 * step), List(ts("sum(true)", 3, 27.0)))
  }

  test(":count expr, c=3") {
    assertEquals(exec(":true,:count", 3 * step), List(ts("count(true)", 3, 5.0)))
  }

  test(":count expr, c=3, cf=sum") {
    assertEquals(exec(":true,:count,:cf-sum", 3 * step), List(ts("count(true)", 3, 15.0)))
  }

  test(":count expr, c=3, cf=max") {
    assertEquals(exec(":true,:count,:cf-max", 3 * step), List(ts("count(true)", 3, 5.0)))
  }

  test(":min expr, c=3") {
    assertEquals(exec(":true,:min", 3 * step), List(ts("min(true)", 3, 1.0)))
  }

  test(":max expr, c=3") {
    assertEquals(exec(":true,:max", 3 * step), List(ts("max(true)", 3, 8.0)))
  }

  test(":by expr, c=3") {
    val expected = List(
      expTS("a", "(name=a)", 3, 2.0),
      expTS("b", "(name=b)", 3, 2.0),
      expTS("c", "(name=c)", 3, 18.0)
    )
    assertEquals(exec(":true,(,name,),:by", 3 * step), expected)
  }

  test(":all expr, c=3") {
    val expected = List(
      expTS("a", "name=a", 3, 6.0),
      expTS("b", "name=b", 3, 6.0),
      expTS("c", "name=c", 3, 54.0)
    )
    assertEquals(exec(":true,:all", 3 * step), expected)
  }

  test("filter") {
    assertEquals(exec("name,(,a,b,),:in"), List(ts("sum(name in (a,b))", 1, 4.0, 4.0, 4.0)))
    clock.setWallTime(4 * step)
    db.setFilter(Query.Equal("name", "a"))
    db.rebuild()
    assertEquals(exec("name,(,a,b,),:in"), List(ts("sum(name in (a,b))", 1, 1.0, 2.0, 3.0)))
  }
}
