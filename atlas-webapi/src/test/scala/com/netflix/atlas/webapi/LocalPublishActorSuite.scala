/*
 * Copyright 2014-2019 Netflix, Inc.
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
package com.netflix.atlas.webapi

import com.netflix.atlas.core.db.MemoryDatabase
import com.netflix.atlas.core.index.TagQuery
import com.netflix.atlas.core.model.Block
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.TagKey
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.ManualClock
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class LocalPublishActorSuite extends FunSuite {

  private val clock = new ManualClock()
  private val registry = new DefaultRegistry(clock)

  private def newDB =
    new MemoryDatabase(
      registry,
      ConfigFactory.parseString("""
      |block-size = 60
      |num-blocks = 2
      |rebuild-frequency = 10s
      |test-mode = true
      |intern-while-building = true
    """.stripMargin)
    )

  private def newDatapoint(v: Double): Datapoint = {
    Datapoint(Map("name" -> "test", TagKey.rollup -> "true"), clock.wallTime(), v)
  }

  test("dedup counter is incremented") {
    val processor = new LocalPublishActor.DatapointProcessor(registry, newDB)
    val deduped = registry.counter("atlas.db.numDeduped")
    val init = deduped.count()

    processor.update("test", Nil)
    assert(init === deduped.count())

    processor.update("test", Nil)
    assert(init + 1 === deduped.count())
  }

  test("honor atlas.rollup tag") {
    val db = newDB
    val processor = new LocalPublishActor.DatapointProcessor(registry, db)

    processor.update("a", List(newDatapoint(1.0)))
    processor.update("b", List(newDatapoint(2.0)))
    processor.update("c", List(newDatapoint(3.0)))
    processor.update("a", List(newDatapoint(1.0))) // Should get deduped by id

    db.index.rebuildIndex()
    val vs = db.index.findItems(TagQuery(Some(Query.True)))
    assert(vs.size === 1)

    val bs = vs.head.blocks
    assert(bs.blockList.size === 1)

    val b = bs.blockList.head
    assert(b.get(0, Block.Sum) === 6.0)
    assert(b.get(0, Block.Count) === 3.0)
    assert(b.get(0, Block.Min) === 1.0)
    assert(b.get(0, Block.Max) === 3.0)
  }

  test("overcount due to message id limit overflow") {
    val db = newDB
    val processor = new LocalPublishActor.DatapointProcessor(registry, db, maxMessageIds = 2)

    processor.update("a", List(newDatapoint(1.0)))
    processor.update("b", List(newDatapoint(2.0)))
    processor.update("c", List(newDatapoint(3.0)))
    processor.update("a", List(newDatapoint(1.0))) // Not deduped, too many ids

    db.index.rebuildIndex()
    val vs = db.index.findItems(TagQuery(Some(Query.True)))
    assert(vs.size === 1)

    val bs = vs.head.blocks
    assert(bs.blockList.size === 1)

    val b = bs.blockList.head
    assert(b.get(0, Block.Sum) === 7.0)
    assert(b.get(0, Block.Count) === 4.0)
    assert(b.get(0, Block.Min) === 1.0)
    assert(b.get(0, Block.Max) === 3.0)
  }
}
