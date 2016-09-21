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
import akka.actor.ActorRef
import com.netflix.atlas.lwcapi.SubscriptionManager.Entry

import scala.collection.mutable

//
// Mock class to track calls.
//
// Todo: add a way to indicate return calls for methods
//
class MockSubscriptionManager extends SubscriptionManager {
  private val invocationList = mutable.ListBuffer[String]()

  def invocations: List[String] = invocationList.toList
  def resetInvocations(): Unit = invocationList.clear()

  override def register(sseId: String, ref: ActorRef, name: String): Unit = {
    invocationList += s"register,$sseId,$name"
  }

  override def subscribe(sseId: String, expressionId: String): Unit = {
    invocationList += s"subscribe,$sseId,$expressionId"
  }

  override def unsubscribe(sseId: String, expressionId: String): Unit = {
    invocationList += s"unsubscribe,$sseId,$expressionId"
  }

  override def unsubscribeAll(sseId: String): Unit = {
    invocationList += s"unsubscribeAll,$sseId"
  }

  override def getActorsForExpressionId(expressionId: String): Set[ActorRef] = {
    invocationList += s"getActorsForExpressionId,$expressionId"
    Set()
  }

  override def getExpressionsForStreamId(sseId: String): Set[String] = {
    invocationList += s"getExpressionsForStreamId,$sseId"
    Set()
  }

  override def entries: List[Entry] = {
    invocationList += s"entries"
    List()
  }
}

object MockSubscriptionManager {
  def apply() = new MockSubscriptionManager()
}
