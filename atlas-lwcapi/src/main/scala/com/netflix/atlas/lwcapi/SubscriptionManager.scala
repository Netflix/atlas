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

abstract class SubscriptionManager() {
  import SubscriptionManager._

  def register(streamId: String, ref: ActorRef, name: String): Unit
  def registration(streamId: String): Option[Entry]

  def subscribe(streamId: String, expressionId: String): Unit
  def unsubscribe(streamId: String, expressionId: String): Unit
  def unregister(streamId: String): List[String]

  def actorsForExpression(expressionId: String): Set[ActorRef]
  def subscribersForExpression(expressionId: String): Set[String]
  def expressionsForSubscriber(streamId: String): Set[String]
}

object SubscriptionManager {
  case class Entry(streamId: String, actorRef: ActorRef, name: String, connectTime: Long)
}
