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
  * Sanity check prefix matching and the benefits of short ciruiting with a startsWith
  * check before falling back to the regex matcher.
  *
  * ```
  * > run -wi 10 -i 10 -f1 -t1 .*StringMatchingPrefix.*
  * [info] Benchmark                      Mode  Cnt      Score     Error  Units
  * [info] StringMatchingPrefix.matcher  thrpt   10  10314.028 ± 191.544  ops/s
  * [info] StringMatchingPrefix.regex    thrpt   10   3431.614 ± 350.880  ops/s
  * ```
  */
@State(Scope.Thread)
class StringMatchingPrefix {

  private val ids = (0 until 10000).map(_ => UUID.randomUUID().toString).toList

  private val patternWithDot = "^disk.percentused."

  private val regex = Pattern.compile(patternWithDot)
  private val matcher = PatternMatcher.compile(patternWithDot)

  @Threads(1)
  @Benchmark
  def regex(bh: Blackhole): Unit = {
    bh.consume(ids.filter(id => regex.matcher(id).find()))
  }

  @Threads(1)
  @Benchmark
  def matcher(bh: Blackhole): Unit = {
    bh.consume(ids.filter(id => matcher.matches(id)))
  }

}
