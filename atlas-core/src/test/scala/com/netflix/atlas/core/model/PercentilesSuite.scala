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

import java.util.concurrent.TimeUnit
import java.util.stream.Collectors
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.spectator.api.Counter
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.histogram.PercentileBuckets
import com.netflix.spectator.api.histogram.PercentileDistributionSummary
import com.netflix.spectator.api.histogram.PercentileTimer
import munit.FunSuite

import scala.language.postfixOps

class PercentilesSuite extends FunSuite {

  private val interpreter = Interpreter(MathVocabulary.allWords)

  private val start = 0L
  private val step = 60000L
  private val context = EvalContext(start, start + step * 2, step)

  private def ts(bucket: String, values: Double*): TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Gauge, start, step, values.toArray)
    val mode = if (Integer.parseInt(bucket.substring(1), 16) % 2 == 0) "even" else "odd"
    TimeSeries(Map("name" -> "test", "mode" -> mode, "percentile" -> bucket), seq)
  }

  private def parseExpr(str: String): TimeSeriesExpr = {
    interpreter.execute(str).stack match {
      case (v: TimeSeriesExpr) :: _ => v
      case _                        => throw new IllegalArgumentException("invalid expr")
    }
  }

  private def eval(str: String, input: List[TimeSeries]): List[TimeSeries] = {
    val expr = parseExpr(str)
    // Verify we can reparse the string representation and get an identical expression.
    val e2 = parseExpr(expr.toString)
    val e3 = parseExpr(e2.toString)
    assertEquals(e2, e3)
    expr.eval(context, input).data
  }

  private val input100 = {
    (0 until 100).map { i =>
      val bucket = f"D${PercentileBuckets.indexOf(i)}%04X"
      val v = 1.0 / 60.0
      ts(bucket, v, v)
    } toList
  }

  private val inputNaN100 = {
    (0 until 100).map { i =>
      val bucket = f"D${PercentileBuckets.indexOf(i)}%04X"
      val v = 1.0 / 60.0
      ts(bucket, v, Double.NaN)
    } toList
  }

  private val inputBad100 = {
    // simulates bad client that incorrectly encodes the percentile tag
    (0 until 100).map { i =>
      val bucket = f"D${PercentileBuckets.indexOf(i)}%04x"
      val v = 1.0 / 60.0
      ts(bucket, v, v)
    } toList
  }

  private val inputTimer100 = {
    (0 until 100).map { i =>
      val bucket = f"T${PercentileBuckets.indexOf(i)}%04X"
      val v = 1.0 / 60.0
      ts(bucket, v, v)
    } toList
  }

  private val inputNaN = {
    (0 until 100).map { i =>
      val bucket = f"D${PercentileBuckets.indexOf(i)}%04X"
      val v = Double.NaN
      ts(bucket, v, v)
    } toList
  }

  private val inputSpectatorTimer = {
    import scala.jdk.CollectionConverters.*
    val r = new DefaultRegistry()
    val t = PercentileTimer.get(r, r.createId("test"))
    (0 until 100).foreach { i =>
      t.record(i, TimeUnit.MILLISECONDS)
    }

    val counters = r.counters.collect(Collectors.toList[Counter]).asScala.toList
    counters.map { c =>
      val v = c.count / 60.0
      val seq = new ArrayTimeSeq(DsType.Gauge, start, step, Array(v, v))
      val tags = c.id.tags.asScala.map(t => t.key -> t.value).toMap + ("name" -> c.id.name)
      TimeSeries(tags, seq)
    }
  }

  private val inputSpectatorDistSummary = {
    import scala.jdk.CollectionConverters.*
    val r = new DefaultRegistry()
    val t = PercentileDistributionSummary.get(r, r.createId("test"))
    (0 until 100).foreach { i =>
      t.record(i)
    }

    val counters = r.counters.collect(Collectors.toList[Counter]).asScala.toList
    counters.map { c =>
      val v = c.count / 60.0
      val seq = new ArrayTimeSeq(DsType.Gauge, start, step, Array(v, v))
      val tags = c.id.tags.asScala.map(t => t.key -> t.value).toMap + ("name" -> c.id.name)
      TimeSeries(tags, seq)
    }
  }

  test("distribution summary :sum") {
    val data = eval("name,test,:eq,(,9,25,50,90,100,),:percentiles", input100)

    assertEquals(data.size, 5)
    List(9.0, 25.0, 50.0, 90.0).zip(data).foreach {
      case (p, t) =>
        val estimate = t.data(0L)
        assertEquals(t.tags, Map("name" -> "test", "percentile" -> f"$p%5.1f"))
        assertEquals(t.label, f"percentile(name=test, $p%5.1f)")
        assertEqualsDouble(p, estimate, 2.0)
    }
    assertEquals(data.last.label, f"percentile(name=test, 100.0)")
  }

  test("distribution summary, bad data") {
    val e = intercept[IllegalArgumentException] {
      eval("name,test,:eq,(,9,25,50,90,100,),:percentiles", input100 ::: inputBad100)
    }
    assertEquals(e.getMessage, "requirement failed: invalid percentile encoding: [D000A,D000a]")
  }

  test("distribution summary non-finite data") {
    val data = eval("name,test,:eq,(,9,25,50,90,100,),:percentiles", inputNaN100)

    assertEquals(data.size, 5)
    List(9.0, 25.0, 50.0, 90.0).zip(data).foreach {
      case (p, t) =>
        assertEquals(t.tags, Map("name" -> "test", "percentile" -> f"$p%5.1f"))
        assertEquals(t.label, f"percentile(name=test, $p%5.1f)")

        val estimate1 = t.data(0L)
        assertEqualsDouble(p, estimate1, 2.0)

        val estimate2 = t.data(step)
        assert(estimate2.isNaN)
    }
    assertEquals(data.last.label, f"percentile(name=test, 100.0)")
  }

  test("timer :sum") {
    val data = eval("name,test,:eq,(,25,50,90,),:percentiles", inputTimer100)

    assertEquals(data.size, 3)
    List(25.0, 50.0, 90.0).zip(data).foreach {
      case (p, t) =>
        val estimate = t.data(0L)
        assertEquals(t.tags, Map("name" -> "test", "percentile" -> f"$p%5.1f"))
        assertEquals(t.label, f"percentile(name=test, $p%5.1f)")
        assertEqualsDouble(p / 1e9, estimate, 2.0e-9)
    }
  }

  test("spectator distribution summary :sum") {
    val data = eval("name,test,:eq,(,9,25,50,90,100,),:percentiles", inputSpectatorDistSummary)

    assertEquals(data.size, 5)
    List(9.0, 25.0, 50.0, 90.0).zip(data).foreach {
      case (p, t) =>
        val estimate = t.data(0L)
        assertEquals(t.tags, Map("name" -> "test", "percentile" -> f"$p%5.1f"))
        assertEquals(t.label, f"percentile(name=test, $p%5.1f)")
        assertEqualsDouble(p, estimate, 2.0)
    }
    assertEquals(data.last.label, f"percentile(name=test, 100.0)")
  }

  test("spectator timer :sum") {
    val data = eval("name,test,:eq,(,25,50,90,),:percentiles", inputSpectatorTimer)

    assertEquals(data.size, 3)
    List(25.0, 50.0, 90.0).zip(data).foreach {
      case (p, t) =>
        val estimate = t.data(0L)
        assertEquals(t.tags, Map("name" -> "test", "percentile" -> f"$p%5.1f"))
        assertEquals(t.label, f"percentile(name=test, $p%5.1f)")

        // Values were 0 ot 100 recorded in milliseconds, should be reported in seconds
        assertEqualsDouble(p / 1e3, estimate, 2.0e-3)
    }
  }

  private def checkPercentile(v: Double, s: String): Unit = {
    val data = eval(s"name,test,:eq,(,$v,),:percentiles", inputSpectatorTimer)

    assertEquals(data.size, 1)
    List(v).zip(data).foreach {
      case (_, t) =>
        assertEquals(t.tags, Map("name" -> "test", "percentile" -> s))
        assertEquals(t.label, f"percentile(name=test, $s)")
    }
  }

  test("9.99999999th percentile") {
    checkPercentile(9.99999999, "  9.99999999")
  }

  test("99.99th percentile") {
    checkPercentile(99.99, " 99.99")
  }

  test("99.999999th percentile") {
    checkPercentile(99.999999, " 99.999999")
  }

  test("distribution summary :max") {
    val data = eval("name,test,:eq,:max,(,25,50,90,),:percentiles", input100)

    assertEquals(data.size, 3)
    List(25.0, 50.0, 90.0).zip(data).foreach {
      case (p, t) =>
        val estimate = t.data(0L)
        assertEquals(t.tags, Map("name" -> "test", "percentile" -> f"$p%5.1f"))
        assertEquals(t.label, f"percentile(name=test, $p%5.1f)")
        assertEqualsDouble(p, estimate, 2.0)
    }
  }

  test("distribution summary :median") {
    val data = eval("name,test,:eq,:median", input100)

    assertEquals(data.size, 1)
    List(50.0).zip(data).foreach {
      case (p, t) =>
        val estimate = t.data(0L)
        assertEquals(t.tags, Map("name" -> "test", "percentile" -> f"$p%5.1f"))
        assertEquals(t.label, f"percentile(name=test, $p%5.1f)")
        assertEqualsDouble(p, estimate, 2.0)
    }
  }

  test("group by empty") {
    val data = eval("name,test,:eq,(,foo,),:by,(,25,50,90,),:percentiles", input100)
    assertEquals(data.size, 0)
  }

  test("group by with single result") {
    val data = eval("name,test,:eq,(,name,),:by,(,25,50,90,),:percentiles", input100)

    assertEquals(data.size, 3)
    List(25.0, 50.0, 90.0).zip(data).foreach {
      case (p, t) =>
        val estimate = t.data(0L)
        assertEquals(t.tags, Map("name" -> "test", "percentile" -> f"$p%5.1f"))
        assertEquals(t.label, f"percentile((name=test), $p%5.1f)")
        assertEqualsDouble(p, estimate, 2.0)
    }
  }

  test("group by with multiple results") {
    val data = eval("name,test,:eq,(,mode,),:by,(,25,50,90,),:percentiles", input100)

    assertEquals(data.size, 6)
    List(25.0, 50.0, 90.0).zip(data.filter(_.tags("mode") == "even")).foreach {
      case (p, t) =>
        val estimate = t.data(0L)
        assertEquals(t.tags, Map("name" -> "test", "mode" -> "even", "percentile" -> f"$p%5.1f"))
        assertEquals(t.label, f"percentile((mode=even), $p%5.1f)")
        assertEqualsDouble(p, estimate, 10.0)
    }
    List(25.0, 50.0, 90.0).zip(data.filter(_.tags("mode") == "odd")).foreach {
      case (p, t) =>
        val estimate = t.data(0L)
        assertEquals(t.tags, Map("name" -> "test", "mode" -> "odd", "percentile" -> f"$p%5.1f"))
        assertEquals(t.label, f"percentile((mode=odd), $p%5.1f)")
        assertEqualsDouble(p, estimate, 10.0)
    }
  }

  test("group by multi-level") {
    val data = eval(
      "name,test,:eq,(,mode,),:by,(,25,50,90,),:percentiles,:max,(,percentile,),:by",
      input100
    )

    assertEquals(data.size, 3)
    List(25.0, 50.0, 90.0).zip(data).foreach {
      case (p, t) =>
        val estimate = t.data(0L)
        assertEquals(t.tags, Map("name" -> "test", "percentile" -> f"$p%5.1f"))
        assertEquals(t.label, f"(percentile=$p%5.1f)")
        assertEqualsDouble(p, estimate, 10.0)
    }
  }

  test("group by with math") {
    val data = eval("name,test,:eq,(,mode,),:by,(,90,),:percentiles,1000,:mul", input100)
      .sortWith(_.tags("mode") < _.tags("mode"))

    assertEquals(data.size, 2)
    List("even", "odd").zip(data).foreach {
      case (m, t) =>
        assertEquals(t.tags, Map("name" -> "test", "mode" -> m, "percentile" -> " 90.0"))
        assertEquals(t.label, f"(percentile((mode=$m),  90.0) * 1000.0)")
    }
  }

  test("distribution summary empty") {
    val data = eval(":false,(,25,50,90,),:percentiles", input100)
    assertEquals(data.size, 0)
  }

  test("distribution summary NaN") {
    val data = eval(":true,(,25,50,90,),:percentiles", inputNaN)

    assertEquals(data.size, 3)
    List(25.0, 50.0, 90.0).zip(data).foreach {
      case (p, t) =>
        val estimate = t.data(0L)
        assertEquals(t.tags, Map("percentile" -> f"$p%5.1f"))
        assertEquals(t.label, f"percentile(true, $p%5.1f)")
        assert(estimate.isNaN)
    }
  }

  test("bad input: too small") {
    intercept[IllegalArgumentException] {
      eval("name,test,:eq,(,-1,),:percentiles", input100)
    }
  }

  test("bad input: too big") {
    intercept[IllegalArgumentException] {
      eval("name,test,:eq,(,100.1,),:percentiles", input100)
    }
  }

  test("bad input: string in list") {
    intercept[IllegalStateException] {
      eval("name,test,:eq,(,50,foo,),:percentiles", input100)
    }
  }

  test("bad input: unsupported :head") {
    intercept[IllegalStateException] {
      eval("name,test,:eq,(,mode,),:by,4,:head,(,50,),:percentiles", input100)
    }
  }

  test("bad input: unsupported :all") {
    intercept[IllegalStateException] {
      eval("name,test,:eq,:all,(,50,),:percentiles", input100)
    }
  }

  test("bad input: response data does not have percentile tag") {
    val input = input100.map(t => t.withTags(t.tags - TagKey.percentile))
    val data = eval("name,test,:eq,(,50,),:percentiles", input)
    assert(data.isEmpty)
  }

  test("bad input: no matches") {
    val data = eval("name,test,:eq,(,50,),:percentiles", Nil)
    assert(data.isEmpty)
  }

  test("bad input: DataExpr -> NoDataLine") {
    val by = DataExpr.GroupBy(DataExpr.Sum(Query.True), List("percentile"))
    val expr = MathExpr.Percentiles(by, List(50.0))
    val input = Map[DataExpr, List[TimeSeries]](by -> List(TimeSeries.noData(step)))
    val ts = expr.eval(context, input).data
    assertEquals(ts.size, 1)
    assertEquals(ts.head.tags, Map("name" -> "NO_DATA"))
    assertEquals(ts.head.label, "NO DATA")
  }

  test("sample-count: distribution summary, range") {
    val data = eval("name,test,:eq,50,100,:sample-count", input100)
    assertEquals(data.size, 1)
    val t = data.head
    assertEqualsDouble(t.data(0L), 0.9, 1e-6)
  }

  test("sample-count: distribution summary, 0 - N") {
    val data = eval("name,test,:eq,0,50,:sample-count", input100)
    assertEquals(data.size, 1)
    val t = data.head
    assertEqualsDouble(t.data(0L), 0.85, 1e-6)
  }

  test("sample-count: distribution summary, N - Max") {
    val data = eval("name,test,:eq,50,Infinity,:sample-count", input100)
    assertEquals(data.size, 1)
    val t = data.head
    assertEqualsDouble(t.data(0L), 0.9, 1e-6)
  }

  test("sample-count: distribution summary, Min >= Max") {
    val e = intercept[IllegalArgumentException] {
      eval("name,test,:eq,5,5,:sample-count", input100)
    }
    assertEquals(e.getMessage, "requirement failed: min >= max (min=5.0, max=5.0)")
  }

  test("sample-count: distribution summary, Min < 0") {
    val e = intercept[IllegalArgumentException] {
      eval("name,test,:eq,-5,5,:sample-count", input100)
    }
    assertEquals(e.getMessage, "requirement failed: min < 0 (min=-5.0)")
  }

  test("sample-count: distribution summary, NaN - 100") {
    val e = intercept[IllegalArgumentException] {
      eval("name,test,:eq,NaN,100,:sample-count", input100)
    }
    assertEquals(e.getMessage, "requirement failed: min >= max (min=NaN, max=100.0)")
  }

  test("sample-count: distribution summary, 0 - NaN") {
    val e = intercept[IllegalArgumentException] {
      eval("name,test,:eq,0,NaN,:sample-count", input100)
    }
    assertEquals(e.getMessage, "requirement failed: min >= max (min=0.0, max=NaN)")
  }

  test("sample-count: distribution summary, NaN - NaN") {
    val e = intercept[IllegalArgumentException] {
      eval("name,test,:eq,NaN,NaN,:sample-count", input100)
    }
    assertEquals(e.getMessage, "requirement failed: min >= max (min=NaN, max=NaN)")
  }

  test("sample-count: timer, range too high") {
    // Timer range is in seconds, sample data is 0-100 ns
    val data = eval("name,test,:eq,50,100,:sample-count", inputTimer100)
    assertEquals(data.size, 1)
    val t = data.head
    assert(t.data(0L).isNaN)
  }

  test("sample-count: timer, range") {
    // Timer range is in seconds, sample data is 0-100 ns
    val data = eval("name,test,:eq,50e-9,100e-9,:sample-count", inputTimer100)
    assertEquals(data.size, 1)
    val t = data.head
    assertEqualsDouble(t.data(0L), 0.9, 1e-6)
  }
}
