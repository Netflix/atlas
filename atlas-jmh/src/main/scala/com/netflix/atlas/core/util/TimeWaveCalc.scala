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
package com.netflix.atlas.core.util

import java.time.Duration

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

/**
  * ```
  * > run -bm all -wi 10 -i 10 -f1 -t1 .*TimeWaveCalc.*
  * ...
  * [info] Benchmark                         Mode     Cnt         Score        Error  Units
  * [info] TimeWaveCalc.testMathSin         thrpt      10  11484308.329 ± 729466.777  ops/s
  * [info] TimeWaveCalc.testPrecomputeSin   thrpt      10  39806386.423 ± 868184.835  ops/s
  * [info] TimeWaveCalc.testMathSin          avgt      10        ≈ 10⁻⁷                s/op
  * [info] TimeWaveCalc.testPrecomputeSin    avgt      10        ≈ 10⁻⁸                s/op
  * [info] TimeWaveCalc.testMathSin        sample  110059        ≈ 10⁻⁷                s/op
  * [info] TimeWaveCalc.testPrecomputeSin  sample  163664        ≈ 10⁻⁷                s/op
  * [info] TimeWaveCalc.testMathSin            ss      10        ≈ 10⁻⁶                s/op
  * [info] TimeWaveCalc.testPrecomputeSin      ss      10        ≈ 10⁻⁶                s/op
  * ```
  */
@State(Scope.Thread)
class TimeWaveCalc {

  private val step = 60000L

  private val dayWave = TimeWave.get(Duration.ofDays(1), step)

  private val lambda = 2 * scala.math.Pi / Duration.ofDays(1).toMillis

  private val timestamp = System.currentTimeMillis() / step * step

  private def mathSin(t: Long): Double = {
    scala.math.sin(t * lambda)
  }

  @Threads(1)
  @Benchmark
  def testMathSin(bh: Blackhole): Unit = {
    bh.consume(mathSin(timestamp))
  }

  @Threads(1)
  @Benchmark
  def testPrecomputeSin(bh: Blackhole): Unit = {
    bh.consume(dayWave(timestamp))
  }
}
