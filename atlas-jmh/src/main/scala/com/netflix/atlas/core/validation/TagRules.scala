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
package com.netflix.atlas.core.validation

import com.netflix.atlas.core.util.SortedTagMap
import com.netflix.spectator.impl.AsciiSet
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

/**
  * TagRule needs to loop over all the entries in the map. This benchmark compares
  * looping over the tags for each rule to looping over the rules for each tag.
  *
  * ```
  * > jmh:run -prof gc -wi 10 -i 10 -f1 -t1 .*TagRules.*
  * ```
  *
  * Throughput
  *
  * ```
  * Benchmark           Mode  Cnt        Score        Error   Units
  * composite          thrpt   10  1345178.780 ± 220318.775   ops/s
  * compositeSorted    thrpt   10  1330052.142 ± 100088.503   ops/s
  * separate           thrpt   10  1827998.619 ± 126933.939   ops/s
  * separateSorted     thrpt   10  2085370.740 ± 262945.382   ops/s
  * ```
  */
@State(Scope.Thread)
class TagRules {

  private val tags = SortedTagMap(
    "nf.app"     -> "atlas_backend",
    "nf.cluster" -> "atlas_backend-dev",
    "nf.asg"     -> "atlas_backend-dev-v001",
    "nf.stack"   -> "dev",
    "nf.region"  -> "us-east-1",
    "nf.zone"    -> "us-east-1e",
    "nf.node"    -> "i-123456789",
    "nf.ami"     -> "ami-987654321",
    "nf.vmtype"  -> "r3.2xlarge",
    "name"       -> "jvm.gc.pause",
    "cause"      -> "Allocation_Failure",
    "action"     -> "end_of_major_GC",
    "statistic"  -> "totalTime"
  )

  private val rules = List(
    KeyLengthRule(2, 80),
    NameValueLengthRule(ValueLengthRule(2, 255), ValueLengthRule(2, 120)),
    ValidCharactersRule(
      AsciiSet.fromPattern("-._A-Za-z0-9"),
      Map.empty.withDefaultValue(AsciiSet.fromPattern("-._A-Za-z0-9"))
    ),
    ReservedKeyRule(
      "nf.",
      Set(
        "nf.app",
        "nf.cluster",
        "nf.asg",
        "nf.stack",
        "nf.region",
        "nf.zone",
        "nf.node",
        "nf.ami",
        "nf.vmtype"
      )
    ),
    ReservedKeyRule("atlas.", Set("legacy"))
  )

  private val composite = CompositeTagRule(rules)

  @Benchmark
  def separate(bh: Blackhole): Unit = {
    bh.consume(Rule.validate(tags, rules))
  }

  @Benchmark
  def composite(bh: Blackhole): Unit = {
    bh.consume(composite.validate(tags))
  }
}
