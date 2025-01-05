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
package com.netflix.atlas.pekko

import org.apache.pekko.http.scaladsl.model.Uri
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

/**
  * Compare different ways to adjust path for a URI.
  *
  * ```
  * > jmh:run -prof stack -prof gc -wi 5 -i 10 -f1 -t1 .*UriParsing.*
  * ...
  * Benchmark                            Mode  Cnt         Score        Error   Units
  * addPath                             thrpt   10   6430310.311 ± 108825.698   ops/s
  * addSegment                          thrpt   10   9973181.589 ± 280533.182   ops/s
  * parse                               thrpt   10    567621.328 ±   8144.481   ops/s
  *
  * Benchmark                            Mode  Cnt         Score        Error   Units
  * addPath                gc.alloc.rate.norm   10       296.060 ±      0.003    B/op
  * addSegment             gc.alloc.rate.norm   10       240.040 ±      0.002    B/op
  * parse                  gc.alloc.rate.norm   10      5680.749 ±      0.019    B/op
  * ```
  */
@State(Scope.Benchmark)
class UriParsing {

  private val addr = "100.123.123.123"
  private val baseUri = Uri(s"http://$addr:7101")

  @Benchmark
  def parse(bh: Blackhole): Unit = {
    bh.consume(Uri(s"http://$addr:7101/api/v1/tags"))
  }

  @Benchmark
  def addSegment(bh: Blackhole): Unit = {
    bh.consume(baseUri.withPath(baseUri.path / "api" / "v1" / "tags"))
  }

  @Benchmark
  def addPath(bh: Blackhole): Unit = {
    bh.consume(baseUri.withPath(baseUri.path + "/api/v1/tags"))
  }

}
