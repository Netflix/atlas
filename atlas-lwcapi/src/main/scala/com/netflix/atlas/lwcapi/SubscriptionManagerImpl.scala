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

case class SubscriptionManagerImpl() extends SubscriptionManager {
  private val subs = scala.collection.mutable.Map[String, Set[ActorRef]]().withDefaultValue(Set())

  def subscribe(expressionId: String, ref: ActorRef): Unit = synchronized {
    subs(expressionId) += ref
  }

  def unsubscribe(expressionId: String, ref: ActorRef): Unit = synchronized {
    subs(expressionId) -= ref
  }

  def get(expressionId: String): Set[ActorRef] = synchronized {
    subs(expressionId)
  }

  def unsubscribeAll(ref: ActorRef): Unit = synchronized {
    subs.keySet.foreach(expressionId => subs(expressionId) -= ref)
  }
}
