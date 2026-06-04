/*
 * Copyright 2014-2026 Netflix, Inc.
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

import com.netflix.atlas.core.index.TagQuery
import com.netflix.atlas.core.model.*
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.ManualClock
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class MemoryDatabaseSuite extends FunSuite {

  private val interpreter = new Interpreter(DataVocabulary.allWords)

  private val step = DefaultSettings.stepSize

  private val start = step

  private val clock = new ManualClock()
  private val registry = new DefaultRegistry(clock)

  private var db: MemoryDatabase = _
  private var gaugeDb: MemoryDatabase = _

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

    // For testing gauge consolidation behavior
    gaugeDb = new MemoryDatabase(
      registry,
      ConfigFactory.parseString("""
          |block-size = 60
          |num-blocks = 2
          |rebuild-frequency = 10s
          |test-mode = true
          |intern-while-building = true
        """.stripMargin)
    )

    addGaugeData("a", 1.0, 2.0, 3.0, 4.0)
    addGaugeData("b", 4.0, 3.0, 2.0, 1.0)
    addGaugeData("c", 3.0, 1.0, Double.NaN, Double.NaN)
    addGaugeData("d", Double.NaN, 4.0, Double.NaN, Double.NaN)
  }

  private val context = EvalContext(start, start + 3 * step, step)

  private def addData(name: String, values: Double*): Unit = {
    val tags = Map("name" -> name)
    val id = TaggedItem.computeId(tags)
    val data = values.toList.zipWithIndex.map {
      case (v, i) =>
        val t = start + i * step
        clock.setWallTime(t)
        DatapointTuple(id, tags, t, v)
    }
    db.update(data)
    db.rebuildIndex()
  }

  private def addRollupData(name: String, values: Double*): Unit = {
    val tags = Map("name" -> name)
    val id = TaggedItem.computeId(tags)
    val data = values.toList.zipWithIndex.map {
      case (v, i) =>
        val t = start + i * step
        clock.setWallTime(t)
        DatapointTuple(id, tags, t, v)
    }
    data.foreach(db.rollup)
    db.rebuildIndex()
  }

  private def addGaugeData(name: String, values: Double*): Unit = {
    val tags = Map("name" -> name, TagKey.dsType -> "gauge")
    val id = TaggedItem.computeId(tags)
    val data = values.toList.zipWithIndex.map {
      case (v, i) =>
        val t = start + i * step
        clock.setWallTime(t)
        DatapointTuple(id, tags, t, v)
    }
    gaugeDb.update(data)
    gaugeDb.rebuildIndex()
  }

  private def expr(str: String): DataExpr = {
    interpreter.execute(str).stack match {
      case ModelDataTypes.DataExprType(v) :: Nil => v
      case _ => throw new IllegalArgumentException(s"invalid data expr: $str")
    }
  }

  private def exec(str: String, s: Long = step): List[TimeSeries] = {
    val ctxt = if (s == step) context else context.copy(start = s, end = 2 * s, step = s)
    db.execute(ctxt, expr(str)).sortWith(_.label < _.label).map { t =>
      t.mapTimeSeq(s => s.bounded(ctxt.start, ctxt.end))
    }
  }

  private def gaugeExec(str: String, s: Long): List[TimeSeries] = {
    val ctxt = if (s == step) context else context.copy(start = s, end = 2 * s, step = s)
    gaugeDb.execute(ctxt, expr(str)).sortWith(_.label < _.label).map { t =>
      t.mapTimeSeq(s => s.bounded(ctxt.start, ctxt.end))
    }
  }

  private def ts(label: String, mul: Int, values: Double*): TimeSeries = {
    val s = start + (mul * step) - step
    TimeSeries(Map.empty, label, new ArrayTimeSeq(DsType.Rate, s, mul * step, values.toArray))
  }

  private def ts(name: String, label: String, mul: Int, values: Double*): TimeSeries = {
    val s = start + (mul * step) - step
    val seq = new ArrayTimeSeq(DsType.Rate, s, mul * step, values.toArray)
    TimeSeries(Map("name" -> name, "foo" -> "bar"), label, seq)
  }

  private def expTS(name: String, label: String, mul: Int, values: Double*): TimeSeries = {
    val tmp = ts(name, label, mul, values*)
    tmp.withTags(tmp.tags - "foo")
  }

  private def gts(label: String, mul: Int, values: Double*): TimeSeries = {
    val s = start + (mul * step) - step
    TimeSeries(Map.empty, label, new ArrayTimeSeq(DsType.Gauge, s, mul * step, values.toArray))
  }

  private def gts(name: String, label: String, mul: Int, values: Double*): TimeSeries = {
    val s = start + (mul * step) - step
    val seq = new ArrayTimeSeq(DsType.Gauge, s, mul * step, values.toArray)
    TimeSeries(Map("name" -> name, "foo" -> "bar"), label, seq)
  }

  private def gaugeTS(name: String, label: String, mul: Int, values: Double*): TimeSeries = {
    val tmp = gts(name, label, mul, values*)
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

  test("gauge :sum expr, c=2, single time series") {
    assertEquals(gaugeExec("name,a,:eq,:sum", 2 * step), List(gaugeTS("a", "sum(name=a)", 2, 1.5)))
    assertEquals(gaugeExec("name,b,:eq,:sum", 2 * step), List(gaugeTS("b", "sum(name=b)", 2, 3.5)))
  }

  test("gauge :sum expr, c=3, single time series") {
    assertEquals(gaugeExec("name,a,:eq,:sum", 3 * step), List(gaugeTS("a", "sum(name=a)", 3, 2.0)))
    assertEquals(gaugeExec("name,b,:eq,:sum", 3 * step), List(gaugeTS("b", "sum(name=b)", 3, 3.0)))
  }

  test("gauge :sum expr, c=4, single time series") {
    assertEquals(gaugeExec("name,a,:eq,:sum", 4 * step), List(gaugeTS("a", "sum(name=a)", 4, 2.5)))
    assertEquals(gaugeExec("name,b,:eq,:sum", 4 * step), List(gaugeTS("b", "sum(name=b)", 4, 2.5)))
  }

  test("gauge :sum expr, c=2, with data") {
    assertEquals(
      gaugeExec("name,(,a,b,),:in,:sum", 2 * step),
      List(gts("sum(name in (a,b))", 2, 5.0))
    )
  }

  test("gauge :sum expr, c=4, single time series with NaN") {
    assertEquals(gaugeExec("name,c,:eq,:sum", 4 * step), List(gaugeTS("c", "sum(name=c)", 4, 2.0)))
    assertEquals(gaugeExec("name,d,:eq,:sum", 4 * step), List(gaugeTS("d", "sum(name=d)", 4, 4.0)))
  }

  test("filter") {
    assertEquals(exec("name,(,a,b,),:in"), List(ts("sum(name in (a,b))", 1, 4.0, 4.0, 4.0)))
    clock.setWallTime(4 * step)
    db.setFilter(Query.Equal("name", "a"))
    db.rebuildIndex()
    assertEquals(exec("name,(,a,b,),:in"), List(ts("sum(name in (a,b))", 1, 1.0, 2.0, 3.0)))
  }

  private def findItems(name: String): List[BlockStoreItem] =
    db.index.findItems(TagQuery(Some(Query.Equal("name", name))))

  test("series revived during rebuild cleanup is not orphaned") {
    val tags = Map("name" -> "racy")
    val id = TaggedItem.computeId(tags)

    // A block store with a single old datapoint that will age out of the window.
    val delegate = new MemoryBlockStore(step, 60, 2)
    delegate.update(start, 1.0)

    // Wrap it to simulate a concurrent update landing in the tiny window between
    // rebuild()'s emptiness check and the entry's removal from data: once armed,
    // the first check that observes an empty store reports "no data" (so removal
    // proceeds) but revives the underlying store as a side effect first, exactly
    // as a racing ingest thread would.
    var revived = false
    val racy = new RacyBlockStore(
      delegate,
      () => {
        // getOrCreateItem still finds this store in `data` and writes to it
        db.update(id, tags, clock.wallTime(), 2.0)
        revived = true
      }
    )

    db.data.put(id, BlockStoreItem.create(id, tags, racy))
    db.rebuildIndex()

    // Present in both the data map and the index to start with.
    assert(db.data.containsKey(id))
    assert(findItems("racy").nonEmpty)

    // Advance well past the retention window, arm the simulated race, and rebuild.
    clock.setWallTime(start + 1000 * step)
    racy.arm()
    db.rebuildIndex()

    assert(revived, "revive callback should fire during the cleanup emptiness check")
    assert(racy.hasData, "store was revived, so it has data again")

    // Index membership is derived from `data`, so a revived store must remain in
    // `data` (kept by the computeIfPresent re-check) and therefore stay queryable;
    // a removed store would simply be absent from both, never an index-only orphan.
    assert(db.data.containsKey(id), "revived store must not be removed from data")
    assert(findItems("racy").nonEmpty, "series should remain queryable and consistent")
  }

  test("rebuild is throttled until rebuild-frequency elapses") {
    val tags = Map("name" -> "throttled")
    val id = TaggedItem.computeId(tags)

    // Establish a known last-build time, then write a new series that is in `data`
    // but not yet in the index.
    clock.setWallTime(start + 3 * step)
    db.rebuildIndex()
    db.update(id, tags, start + 3 * step, 1.0)

    // Within rebuild-frequency (10s) of the last build, rebuild() is a no-op.
    clock.setWallTime(start + 3 * step + 5000)
    db.rebuild()
    assert(findItems("throttled").isEmpty, "rebuild within the throttle window must not rebuild")

    // Past rebuild-frequency, rebuild() runs and indexes the new series.
    clock.setWallTime(start + 3 * step + 11000)
    db.rebuild()
    assert(
      findItems("throttled").nonEmpty,
      "rebuild after the throttle window must index the series"
    )
  }
}

/**
  * Block store wrapper used to deterministically reproduce the race between
  * `rebuild()` cleanup and a concurrent `update()`. Once armed, the first call to
  * `hasData` that observes an empty delegate fires the supplied callback (the
  * simulated concurrent update), then still reports empty -- its value before the
  * revive -- so the cleanup loop proceeds to attempt removal of the now-revived
  * store.
  */
private class RacyBlockStore(delegate: MemoryBlockStore, onEmptyCheck: () => Unit)
    extends BlockStore {

  @volatile private var armed = false

  def arm(): Unit = armed = true

  def hasData: Boolean = {
    val current = delegate.hasData
    if (armed && !current) {
      armed = false
      onEmptyCheck()
      false
    } else {
      current
    }
  }

  def cleanup(cutoff: Long): Unit = delegate.cleanup(cutoff)

  def update(timestamp: Long, value: Double, rollup: Boolean): Unit =
    delegate.update(timestamp, value, rollup)

  def update(timestamp: Long): Unit = delegate.update(timestamp)

  def blockList: List[Block] = delegate.blockList

  def fetch(start: Long, end: Long, aggr: Int): Array[Double] = delegate.fetch(start, end, aggr)

  def get(start: Long): Option[Block] = delegate.get(start)
}
