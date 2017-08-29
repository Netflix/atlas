/*
 * Copyright 2014-2017 Netflix, Inc.
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

import org.scalatest.FunSuite

class SubscriptionManagerSuite extends FunSuite {

  private def sub(expr: String): Subscription = {
    val splitter = new ExpressionSplitter
    splitter.split(expr, 60).head
  }

  test("subscribe, unsubscribe, and get work") {
    val sm = new SubscriptionManager[Integer]()

    val exp1 = sub("name,exp1,:eq")
    val exp2 = sub("name,exp2,:eq")

    val sse1 = "sse1"

    sm.register(sse1, 1)

    sm.subscribe(sse1, exp1)
    assert(sm.handlersForSubscription(exp1.metadata.id) === List(1))

    sm.subscribe(sse1, exp2)
    assert(sm.handlersForSubscription(exp2.metadata.id) === List(1))

    sm.unsubscribe(sse1, exp1.metadata.id)
    assert(sm.handlersForSubscription(exp1.metadata.id) === List.empty)

    assert(sm.unregister(sse1) === Some(1))
    assert(sm.handlersForSubscription(exp1.metadata.id) === List.empty)

    assert(sm.unregister(sse1) === None)
  }

  test("duplicate subscriptions") {
    val sm = new SubscriptionManager[Integer]()
    sm.register("a", 1)

    val s = sub("name,exp1,:eq")
    sm.subscribe("a", s)
    sm.subscribe("a", s)

    assert(sm.subscriptions === List(s))
  }

  test("same subscription for two streams") {
    val sm = new SubscriptionManager[Integer]()
    sm.register("a", 1)
    sm.register("b", 2)

    val s = sub("name,exp1,:eq")
    sm.subscribe("a", s)
    sm.subscribe("b", s)

    assert(sm.subscriptions === List(s))
    assert(sm.subscriptionsForStream("a") === List(s))
    assert(sm.subscriptionsForStream("b") === List(s))
    assert(sm.handlersForSubscription(s.metadata.id).sorted === List(1, 2))
  }

  private def checkSubsForCluster(expr: String, cluster: String): Unit = {
    val sm = new SubscriptionManager[Integer]()
    val s = sub(expr)
    sm.register("a", 1)
    sm.subscribe("a", s)
    sm.regenerateQueryIndex()
    assert(sm.subscriptionsForCluster(cluster) === List(s))
  }

  test("subscriptions for cluster, just name") {
    checkSubsForCluster("name,exp1,:eq", "www-dev")
  }

  test("subscriptions for cluster, app") {
    checkSubsForCluster("name,exp1,:eq,nf.app,www,:eq,:and", "www-dev")
  }

  test("subscriptions for cluster, cluster") {
    checkSubsForCluster("name,exp1,:eq,nf.cluster,www-dev,:eq,:and", "www-dev")
  }

  test("subscriptions for cluster, asg") {
    checkSubsForCluster("name,exp1,:eq,nf.asg,www-dev-v001,:eq,:and", "www-dev")
  }

  test("subscriptions for cluster, stack") {
    checkSubsForCluster("name,exp1,:eq,nf.stack,dev,:eq,:and", "www-dev")
  }

  test("subscribe to unknown stream") {
    val sm = new SubscriptionManager[Integer]()
    intercept[IllegalStateException] {
      sm.subscribe("a", sub("name,foo,:eq"))
    }
  }

  test("unsubscribe from unknown stream") {
    val sm = new SubscriptionManager[Integer]()
    intercept[IllegalStateException] {
      sm.unsubscribe("a", "d")
    }
  }

  test("unsubscript for unknown expression does not cause any exceptions") {
    val sm = new SubscriptionManager[Integer]()
    sm.register("a", 42)
    sm.unsubscribe("a", "d")
  }

  test("unregister for unknown stream does not cause any exceptions") {
    val sm = new SubscriptionManager[Integer]()
    assert(sm.unregister("a") === None)
  }
}
