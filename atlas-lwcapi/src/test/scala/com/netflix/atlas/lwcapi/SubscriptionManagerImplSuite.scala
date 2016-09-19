/*
 * Copyright 2014-2016 Netflix, Inc.
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

import akka.actor.{Actor, ActorSystem, Props}
import org.scalatest.FunSuite

class SubscriptionManagerImplSuite() extends FunSuite {
  test("subscribe, unsubscribe, and get work") {
    val system = ActorSystem("HelloSystem")

    val sm = SubscriptionManagerImpl()
    val tag1 = "tag1"
    val tag2 = "tag2"
    val tag3 = "tag3"
    val ref1 = system.actorOf(Props(new TestActor(sm)), name = "ref1")
    val ref2 = system.actorOf(Props(new TestActor(sm)), name = "ref2")

    sm.subscribe(tag1, ref1)
    assert(sm.get(tag1) === Set(ref1))

    sm.subscribe(tag1, ref2)
    assert(sm.get(tag1) === Set(ref1, ref2))

    assert(sm.get(tag2) === Set())

    sm.unsubscribe(tag1, ref1)
    assert(sm.get(tag1) === Set(ref2))

    sm.unsubscribeAll(ref2)
    assert(sm.get(tag1) === Set())

    sm.subscribe(tag3, ref1)
    ref1 ! "terminate"
    Thread.sleep(500) // Todo:  need a better way to do this...
    assert(sm.get(tag3) === Set())
  }

  class TestActor(subscriptionManager: SubscriptionManagerImpl) extends Actor {
    def receive = {
      case x: String =>
        context.stop(self)
  }

    override def postStop() = {
      subscriptionManager.unsubscribeAll(self)
      super.postStop()
    }
  }
}
