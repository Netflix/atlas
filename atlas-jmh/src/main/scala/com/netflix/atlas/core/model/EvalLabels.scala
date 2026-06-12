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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.stacklang.Interpreter
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

/**
  * Measures the cost of evaluating a grouped math expression that combines two aggregates
  * with a division and an absolute value, over a dataset with ~1k lines per side. This
  * exercises the real eval pipeline (group by, binary op, unary op) and the per-series label
  * construction that goes with it. The default label tries to represent the expression tree,
  * which shows up as a notable source of allocation (`java.util.Formatter` and friends) in
  * production allocation profiles.
  *
  * Two scenarios matter:
  *
  *   - `withLabels`  the resulting labels are consumed (e.g. no simple/explicit legend set).
  *   - `noLabels`    the labels are never read, which is the common case now that simple
  *                   legends are the default. Work spent building those labels is pure waste.
  *
  * ```
  * > jmh:run -wi 10 -i 10 -f1 -t1 -prof gc .*EvalLabels.*
  * ```
  */
@State(Scope.Thread)
class EvalLabels {

  private val n = 1000

  private val start = 0L
  private val step = 60000L
  private val context = EvalContext(start, start + step, step)

  // abs(requests grouped by node / errors grouped by node)
  private val expr: TimeSeriesExpr = {
    val program = "name,requestsPerSecond,:eq,:sum,(,nf.node,),:by," +
      "name,errorsPerSecond,:eq,:sum,(,nf.node,),:by,:div,:abs"
    val interpreter = new Interpreter(MathVocabulary.allWords)
    interpreter.execute(program).stack match {
      case ModelDataTypes.TimeSeriesExprType(t) :: Nil => t
      case _                                           => throw new IllegalStateException(program)
    }
  }

  // ~1k distinct nodes per metric, aligned so the division produces ~1k result lines
  private val input: List[TimeSeries] = {
    val builder = List.newBuilder[TimeSeries]
    var i = 0
    while (i < n) {
      val node = f"i-$i%08x"
      val common = Map(
        "nf.app"     -> "foo-main",
        "nf.cluster" -> "foo-main",
        "nf.asg"     -> "foo-main-v042",
        "nf.region"  -> "us-east-1",
        "nf.zone"    -> "us-east-1a",
        "nf.node"    -> node
      )
      val reqData = new ArrayTimeSeq(DsType.Gauge, start, step, Array(100.0))
      val errData = new ArrayTimeSeq(DsType.Gauge, start, step, Array(2.0))
      builder += TimeSeries(common + ("name" -> "requestsPerSecond"), reqData)
      builder += TimeSeries(common + ("name" -> "errorsPerSecond"), errData)
      i += 1
    }
    builder.result()
  }

  @Threads(1)
  @Benchmark
  def withLabels(bh: Blackhole): Unit = {
    val rs = expr.eval(context, input)
    rs.data.foreach(t => bh.consume(t.label))
  }

  @Threads(1)
  @Benchmark
  def noLabels(bh: Blackhole): Unit = {
    val rs = expr.eval(context, input)
    rs.data.foreach(t => bh.consume(t.data))
  }
}
