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
  // The list of expression IDs mapped to ActorRefs
  private val expressionIdToActorRef = scala.collection.mutable.Map[String, Set[ActorRef]]().withDefaultValue(Set())

  // The list of sseIds mapped to expression IDs
  private val sseIdToExpressionId = scala.collection.mutable.Map[String, Set[String]]().withDefaultValue(Set())

  def subscribe(expressionId: String, sseId: String, ref: ActorRef): Unit = synchronized {
    expressionIdToActorRef(expressionId) += ref
    sseIdToExpressionId(sseId) += expressionId
  }

  def unsubscribe(expressionId: String, sseId: String, ref: ActorRef): Unit = synchronized {
    expressionIdToActorRef(expressionId) -= ref
    sseIdToExpressionId(sseId) -= expressionId
  }

  def getActorsForExpressionId(expressionId: String): Set[ActorRef] = synchronized {
    expressionIdToActorRef(expressionId)
  }

  def getExpressionsForSSEId(sseId: String): Set[String] = synchronized {
    sseIdToExpressionId(sseId)
  }

  def unsubscribeAll(sseId: String, ref: ActorRef): Unit = synchronized {
    expressionIdToActorRef.keySet.foreach(expressionId => expressionIdToActorRef(expressionId) -= ref)
    sseIdToExpressionId.remove(sseId)
  }
}
