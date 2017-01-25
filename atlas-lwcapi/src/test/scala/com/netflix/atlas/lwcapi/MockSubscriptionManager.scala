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
import akka.actor.ActorRef

import scala.collection.mutable

//
// Mock class to track calls.
//
// Todo: add a way to indicate return calls for methods
//
class MockSubscriptionManager extends SubscriptionManager {
  import SubscriptionManager._

  private val invocationList = mutable.ListBuffer[String]()

  def invocations: List[String] = invocationList.toList
  def resetInvocations(): Unit = invocationList.clear()

  override def register(streamId: String, ref: ActorRef, name: String): Unit = {
    invocationList += s"register,$streamId,$name"
  }

  override def registration(streamId: String): Option[Entry] = {
    invocationList += s"getRegistration,$streamId"
    None
  }

  override def subscribe(streamId: String, expressionId: String): Unit = {
    invocationList += s"subscribe,$streamId,$expressionId"
  }

  override def unsubscribe(streamId: String, expressionId: String): Unit = {
    invocationList += s"unsubscribe,$streamId,$expressionId"
  }

  override def unregister(streamId: String): List[String] = {
    invocationList += s"unsubscribeAll,$streamId"
    List()
  }

  override def actorsForExpression(expressionId: String): Set[ActorRef] = {
    invocationList += s"actorsForExpression,$expressionId"
    Set()
  }

  override def subscribersForExpression(expressionId: String): Set[String] = {
    invocationList += s"subscribersForExpression,$expressionId"
    Set()
  }

  override def expressionsForSubscriber(streamId: String): Set[String] = {
    invocationList += s"expressionsForSubscriber,$streamId"
    Set()
  }
}

object MockSubscriptionManager {
  def apply() = new MockSubscriptionManager
}
