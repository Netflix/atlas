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
package com.netflix.atlas.lwcapi

import com.netflix.atlas.eval.model.ExprType
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.ManualClock
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.impl.StepLong
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class SubscriptionManagerSuite extends FunSuite {

  private val config = ConfigFactory.load()

  private def sub(expr: String): Subscription = {
    val splitter = new ExpressionSplitter(config)
    splitter.split(expr, ExprType.TIME_SERIES, 60).head
  }

  test("subscribe, unsubscribe, and get work") {
    val sm = new SubscriptionManager[Integer](new NoopRegistry)

    val exp1 = sub("name,exp1,:eq")
    val exp2 = sub("name,exp2,:eq")

    val sse1 = "sse1"

    sm.register(StreamMetadata(sse1), 1)

    sm.subscribe(sse1, exp1)
    assertEquals(sm.handlersForSubscription(exp1.metadata.id), List(Integer.valueOf(1)))

    sm.subscribe(sse1, exp2)
    assertEquals(sm.handlersForSubscription(exp2.metadata.id), List(Integer.valueOf(1)))

    sm.unsubscribe(sse1, List(exp1.metadata.id))
    assertEquals(sm.handlersForSubscription(exp1.metadata.id), List.empty)

    assertEquals(sm.unregister(sse1), Some(Integer.valueOf(1)))
    assertEquals(sm.handlersForSubscription(exp1.metadata.id), List.empty)

    assertEquals(sm.unregister(sse1), None)
  }

  test("multiple registrations") {
    val sm = new SubscriptionManager[Integer](new NoopRegistry)
    val meta = StreamMetadata("a")
    assert(sm.register(meta, 1))
    assert(!sm.register(meta, 1))
    assertEquals(sm.unregister(meta.streamId), Some(Integer.valueOf(1)))
    assert(sm.register(meta, 1))
  }

  test("subs are maintained on attempted re-register") {
    val meta = StreamMetadata("a")
    val sm = new SubscriptionManager[Integer](new NoopRegistry)
    assert(sm.register(meta, 1))

    val exp1 = sub("name,exp1,:eq")
    sm.subscribe(meta.streamId, exp1)
    sm.updateQueryIndex()
    assertEquals(sm.subscriptions, List(exp1))

    assert(!sm.register(meta, 1))
    sm.updateQueryIndex()
    assertEquals(sm.subscriptions, List(exp1))
  }

  test("multiple subscriptions for stream") {
    val meta = StreamMetadata("a")
    val sm = new SubscriptionManager[Integer](new NoopRegistry)
    sm.register(meta, 1)

    val subs = List(sub("name,exp1,:eq"), sub("name,exp2,:eq"))
    sm.subscribe(meta.streamId, subs)
    sm.updateQueryIndex()

    assertEquals(sm.subscriptions.toSet, subs.toSet)
  }

  test("duplicate subscriptions") {
    val meta = StreamMetadata("a")
    val sm = new SubscriptionManager[Integer](new NoopRegistry)
    sm.register(meta, 1)

    val s = sub("name,exp1,:eq")
    sm.subscribe(meta.streamId, s)
    sm.subscribe(meta.streamId, s)
    sm.updateQueryIndex()

    assertEquals(sm.subscriptions, List(s))
  }

  test("same subscription for two streams") {
    val a = StreamMetadata("a")
    val b = StreamMetadata("b")
    val sm = new SubscriptionManager[Integer](new NoopRegistry)
    sm.register(a, 1)
    sm.register(b, 2)

    val s = sub("name,exp1,:eq")
    sm.subscribe(a.streamId, s)
    sm.subscribe(b.streamId, s)
    sm.updateQueryIndex()

    assertEquals(sm.subscriptions, List(s))
    assertEquals(sm.subscriptionsForStream(a.streamId), List(s))
    assertEquals(sm.subscriptionsForStream(b.streamId), List(s))
    assertEquals(
      sm.handlersForSubscription(s.metadata.id).sorted,
      List(Integer.valueOf(1), Integer.valueOf(2))
    )
  }

  private def subsForCluster(expr: String, cluster: String): List[String] = {
    val sm = new SubscriptionManager[Integer](new NoopRegistry)
    val s = sub(expr)
    sm.register(StreamMetadata("a"), 1)
    sm.subscribe("a", s)
    sm.updateQueryIndex()
    sm.subscriptionsForCluster(cluster).map(_.metadata.expression)
  }

  private def checkSubsForCluster(expr: String, cluster: String): Unit = {
    assertEquals(subsForCluster(expr, cluster), List(expr))
  }

  test("subscriptions for cluster, just name") {
    checkSubsForCluster("name,exp1,:eq,:sum", "www-dev")
  }

  test("subscriptions for cluster, app") {
    checkSubsForCluster("name,exp1,:eq,nf.app,www,:eq,:and,:sum", "www-dev")
  }

  test("subscriptions for cluster, cluster") {
    checkSubsForCluster("name,exp1,:eq,nf.cluster,www-dev,:eq,:and,:sum", "www-dev")
  }

  test("subscriptions for cluster, cluster no match") {
    assertEquals(subsForCluster("name,exp1,:eq,nf.cluster,www-dev,:eq,:and,:sum", "foo-dev"), Nil)
  }

  test("subscriptions for cluster, asg") {
    checkSubsForCluster("name,exp1,:eq,nf.asg,www-dev-v001,:eq,:and,:sum", "www-dev")
  }

  test("subscriptions for cluster, asg no match") {
    assertEquals(subsForCluster("name,exp1,:eq,nf.asg,www-dev-v001,:eq,:and,:sum", "foo"), Nil)
  }

  test("subscriptions for cluster, stack") {
    checkSubsForCluster("name,exp1,:eq,nf.stack,dev,:eq,:and,:sum", "www-dev")
  }

  test("subscriptions for cluster, stack no match") {
    assertEquals(subsForCluster("name,exp1,:eq,nf.stack,dev2,:eq,:and,:sum", "www-dev"), Nil)
  }

  test("subscriptions for cluster, shard1") {
    checkSubsForCluster("name,exp1,:eq,nf.shard1,foo,:eq,:and,:sum", "www-dev-x1foo")
  }

  test("subscriptions for cluster, shard1 no match") {
    assertEquals(subsForCluster("name,exp1,:eq,nf.shard1,foo2,:eq,:and,:sum", "www-dev-x1foo"), Nil)
  }

  test("subscriptions for cluster, shard2") {
    checkSubsForCluster("name,exp1,:eq,nf.shard2,bar,:eq,:and,:sum", "www-dev-x1foo-x2bar")
  }

  test("subscriptions for cluster, shard2 no match") {
    assertEquals(
      subsForCluster("name,exp1,:eq,nf.shard2,bar2,:eq,:and,:sum", "www-dev-x1foo-x2bar"),
      Nil
    )
  }

  test("subscribe to unknown stream") {
    val sm = new SubscriptionManager[Integer](new NoopRegistry)
    intercept[IllegalStateException] {
      sm.subscribe("a", sub("name,foo,:eq"))
    }
  }

  test("unsubscribe from unknown stream") {
    val sm = new SubscriptionManager[Integer](new NoopRegistry)
    intercept[IllegalStateException] {
      sm.unsubscribe("a", List("d"))
    }
  }

  test("unsubscribe for unknown expression does not cause any exceptions") {
    val sm = new SubscriptionManager[Integer](new NoopRegistry)
    sm.register(StreamMetadata("a"), 42)
    sm.unsubscribe("a", List("d"))
  }

  test("unregister for unknown stream does not cause any exceptions") {
    val sm = new SubscriptionManager[Integer](new NoopRegistry)
    assertEquals(sm.unregister("a"), None)
  }

  test("unregister should remove handlers") {
    val sm = new SubscriptionManager[Integer](new NoopRegistry)
    sm.register(StreamMetadata("a"), 1)

    val s = sub("name,exp1,:eq")
    sm.subscribe("a", s)
    assertEquals(sm.handlersForSubscription(s.metadata.id), List(Integer.valueOf(1)))

    sm.unregister("a")
    assertEquals(sm.handlersForSubscription(s.metadata.id), Nil)
  }

  test("subscribe returns added expressions") {
    val sm = new SubscriptionManager[Integer](new NoopRegistry)
    assert(sm.register(StreamMetadata("a"), 1))
    assert(sm.register(StreamMetadata("b"), 2))

    val s1 = sub("name,exp1,:eq")
    val s2 = sub("name,exp2,:eq")
    val s3 = sub("name,exp3,:eq")

    assertEquals(sm.subscribe("a", List(s1, s2)), Integer.valueOf(1)     -> List(s1, s2))
    assertEquals(sm.subscribe("a", List(s1, s2)), Integer.valueOf(1)     -> Nil)
    assertEquals(sm.subscribe("b", List(s1, s2)), Integer.valueOf(2)     -> List(s1, s2))
    assertEquals(sm.subscribe("b", List(s1, s2, s3)), Integer.valueOf(2) -> List(s3))
    assertEquals(sm.subscribe("a", List(s1, s3)), Integer.valueOf(1)     -> List(s3))
  }

  test("update gauges") {
    val clock = new ManualClock()
    val step = 60_000L
    val registry = new DefaultRegistry(clock)

    val sm = new SubscriptionManager[Integer](registry)
    val meta =
      StreamMetadata("a", "test", clock, new StepLong(0, clock, step), new StepLong(0, clock, step))
    assert(sm.register(meta, 1))

    val ok = Id.create("atlas.lwcapi.currentStreams").withTag("state", "ok")
    val dropping = Id.create("atlas.lwcapi.currentStreams").withTag("state", "dropping")

    sm.updateGauges()
    assertEquals(1.0, registry.gauge(ok).value())
    assertEquals(0.0, registry.gauge(dropping).value())

    meta.updateReceived(42)
    meta.updateDropped(7)
    clock.setWallTime(step)
    sm.updateGauges()
    assertEquals(0.0, registry.gauge(ok).value())
    assertEquals(1.0, registry.gauge(dropping).value())
  }
}
