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
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable

case class SubscriptionManagerImpl() extends SubscriptionManager with StrictLogging {
  import SubscriptionManager._

  // The list of expression IDs mapped to ActorRefs
  private val expressionIdToActorRef = mutable.Map[String, Set[ActorRef]]().withDefaultValue(Set())

  // The list of sseIds mapped to expression IDs
  private val sseIdToExpressionId = mutable.Map[String, Set[String]]().withDefaultValue(Set())

  private val sseEntries = mutable.Map[String, Entry]()

  def register(sseId: String, ref: ActorRef, name: String): Unit = {
    sseEntries(sseId) = Entry(sseId, ref, name, System.currentTimeMillis())
  }

  def subscribe(sseId: String, expressionId: String): Unit = synchronized {
    val entry = sseEntries.get(sseId)
    if (entry.nonEmpty)
      expressionIdToActorRef(expressionId) += entry.get.actorRef
    sseIdToExpressionId(sseId) += expressionId
  }

  def unsubscribe(sseId: String, expressionId: String): Unit = synchronized {
    val entry = sseEntries.get(sseId)
    if (entry.nonEmpty)
      expressionIdToActorRef(expressionId) -= entry.get.actorRef
    sseIdToExpressionId(sseId) -= expressionId
  }

  def unsubscribeAll(sseId: String): Unit = synchronized {
    val entry = sseEntries.get(sseId)
    if (entry.nonEmpty)
      expressionIdToActorRef.keySet.foreach(id => expressionIdToActorRef(id) -= entry.get.actorRef)
    sseIdToExpressionId.remove(sseId)
  }

  def getActorsForExpressionId(expressionId: String): Set[ActorRef] = synchronized {
    expressionIdToActorRef(expressionId)
  }

  def getExpressionsForSSEId(sseId: String): Set[String] = synchronized {
    sseIdToExpressionId(sseId)
  }

  def entries: List[Entry] = synchronized { sseEntries.values.toList }

  override def getAllExpressions: Map[String, Set[String]] = synchronized {
    sseIdToExpressionId.toMap
  }
}
