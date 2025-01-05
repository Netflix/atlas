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
package com.netflix.atlas.lwc.events

import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Utils
import com.typesafe.config.ConfigFactory
import munit.FunSuite

import java.util.concurrent.CopyOnWriteArrayList

class RemoveLwcEventClientSuite extends FunSuite {

  private val config = ConfigFactory.load()
  private var payloads: java.util.List[RemoteLwcEventClient.EvalPayload] = _
  private var registry: Registry = _
  private var client: RemoteLwcEventClient = _

  override def beforeEach(context: BeforeEach): Unit = {
    payloads = new CopyOnWriteArrayList[RemoteLwcEventClient.EvalPayload]()
    registry = new DefaultRegistry()
    client = new RemoteLwcEventClient(registry, config) {
      override def start(): Unit = {
        val subs = Subscriptions(events =
          List(
            Subscription(
              "test",
              0L,
              ":true",
              "EVENTS"
            )
          )
        )
        sync(subs)
      }

      override protected def send(payload: RemoteLwcEventClient.EvalPayload): Unit = {
        payloads.add(payload)
      }
    }
  }

  test("batch events by size") {
    val str = "1234567890" * 100_000
    val event = LwcEvent(str, _ => str)
    val events = (0 until 100).map(_ => RemoteLwcEventClient.Event("_", event)).toList
    val results = List.newBuilder[List[RemoteLwcEventClient.Event]]
    client.batch(events, results.addOne)

    val batches = results.result()
    assertEquals(batches.size, 100 / 7 + 1)
    batches.foreach { batch =>
      assert(batch.size <= 7)
    }
  }

  test("batch events, single event is too big") {
    val str = "1234567890" * 1_000_000
    val event = LwcEvent(str, _ => str)
    val events = List(RemoteLwcEventClient.Event("_", event))
    val results = List.newBuilder[List[RemoteLwcEventClient.Event]]
    client.batch(events, results.addOne)

    val batches = results.result()
    assertEquals(batches.size, 0)

    val errors = registry
      .counters()
      .filter(c => c.id.name == "lwc.events" && Utils.getTagValue(c.id, "error") == "too-big")
      .toList
    assertEquals(errors.size(), 1)
    errors.forEach { c =>
      assertEquals(c.count(), 1L)
    }
  }

  test("flush on heartbeat") {
    client.start()

    val str = "1234567890"
    val event = LwcEvent(str, _ => str)
    val events = (0 until 100).map(_ => event).toList

    events.foreach(client.process)
    assert(payloads.isEmpty)

    client.process(LwcEvent.HeartbeatLwcEvent(registry.clock().wallTime()))
    assertEquals(payloads.size(), 1)
    assertEquals(payloads.get(0).size, 100)
  }
}
