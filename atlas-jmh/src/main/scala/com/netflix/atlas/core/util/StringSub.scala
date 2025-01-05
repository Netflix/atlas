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

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

/**
  * Quick sanity check on substitute changes. Main goal is to reduce some of the allocations seen
  * in batch use-cases.
  *
  * ```
  * > run -wi 10 -i 10 -f1 -t1 .*StringSub.*
  * ```
  *
  * Initial results:
  *
  * ```
  * [info] Benchmark                     Mode  Cnt        Score       Error  Units
  * [info] StringSub.testParsingByHand  thrpt   10  2223516.436 ± 43697.844  ops/s
  * [info] StringSub.testUsingRegex     thrpt   10   267252.094 ±  6066.379  ops/s
  * ```
  */
@State(Scope.Thread)
class StringSub {

  /**
    * Simple variable syntax with $varname.
    */
  private val SimpleVar = """\$([-_.a-zA-Z0-9]+)""".r

  /**
    * Simple variable syntax where variable name is enclosed in parenthesis,
    * e.g., $(varname).
    */
  private val ParenVar = """\$\(([^\(\)]+)\)""".r

  def substitute(str: String, vars: Map[String, String]): String = {
    import scala.util.matching.Regex

    def f(m: Regex.Match): String = vars.getOrElse(m.group(1), m.group(1))
    val tmp = SimpleVar.replaceAllIn(str, f _)
    ParenVar.replaceAllIn(tmp, f _)
  }

  private val legend = "$(nf.cluster), $nf.asg, $nf.zone, $(name)"

  private val tags = SortedTagMap(
    "nf.app"     -> "foo-main",
    "nf.cluster" -> "foo-main",
    "nf.asg"     -> "foo-main-v042",
    "nf.region"  -> "us-east-1",
    "nf.zone"    -> "us-east-1a",
    "nf.ami"     -> "ami-12345678",
    "nf.node"    -> "i-87654321",
    "name"       -> "snmp.ssCpuUtilization",
    "device"     -> "ps3",
    "country"    -> "US"
  )

  @Threads(1)
  @Benchmark
  def testUsingRegex(bh: Blackhole): Unit = {
    bh.consume(substitute(legend, tags))
  }

  @Threads(1)
  @Benchmark
  def testParsingByHand(bh: Blackhole): Unit = {
    bh.consume(Strings.substitute(legend, tags))
  }
}
