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

import java.util.UUID
import java.util.regex.Pattern

import com.netflix.spectator.impl.PatternMatcher
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

/**
  * Regex OR queries have terrible performance. In many cases these can be rewritten to
  * a set of startsWith or indexOf patterns in the string matcher. On some of the real
  * data and queries this was up to 20 times faster. For the quick sample data in this
  * benchmark it is 8 to 15 times faster.
  *
  * ```
  * > run -wi 10 -i 10 -f1 -t1 .*StringMatchingOr.*
  * [info] Benchmark                              Mode  Cnt    Score    Error  Units
  * [info] StringMatchingOr.testOrMatcher08      thrpt   10  953.969 ± 57.275  ops/s
  * [info] StringMatchingOr.testOrMatcher32      thrpt   10  437.536 ± 32.653  ops/s
  * [info] StringMatchingOr.testRegex08          thrpt   10  117.730 ± 18.946  ops/s
  * [info] StringMatchingOr.testRegex32          thrpt   10   29.162 ±  1.151  ops/s
  * ```
  */
@State(Scope.Thread)
class StringMatchingOr {

  private val ids = (0 until 10000).map(_ => UUID.randomUUID().toString).toList

  private val query8 = "adec123|abc|2|23|12345|abc34521|fedbca*.|98a2def.*"
  private val query32 = s"$query8|$query8|$query8|$query8"

  private val regex8 = Pattern.compile(query8)
  private val orMatcher8 = PatternMatcher.compile(query8)

  private val regex32 = Pattern.compile(query32)
  private val orMatcher32 = PatternMatcher.compile(query32)

  @Threads(1)
  @Benchmark
  def testRegex08(bh: Blackhole): Unit = {
    bh.consume(ids.filter(id => regex8.matcher(id).find()))
  }

  @Threads(1)
  @Benchmark
  def testOrMatcher08(bh: Blackhole): Unit = {
    bh.consume(ids.filter(id => orMatcher8.matches(id)))
  }

  @Threads(1)
  @Benchmark
  def testRegex32(bh: Blackhole): Unit = {
    bh.consume(ids.filter(id => regex32.matcher(id).find()))
  }

  @Threads(1)
  @Benchmark
  def testOrMatcher32(bh: Blackhole): Unit = {
    bh.consume(ids.filter(id => orMatcher32.matches(id)))
  }

}
