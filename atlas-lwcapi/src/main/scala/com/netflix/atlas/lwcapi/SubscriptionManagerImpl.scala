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

  private val exprToSource = mutable.Map[String, Set[String]]().withDefaultValue(Set())

  // The list of expression IDs mapped to ActorRefs
  private val expressionIdToActorRef = mutable.Map[String, Set[ActorRef]]().withDefaultValue(Set())

  // The list of streamIds mapped to expression IDs
  private val streamIdToExpressionId = mutable.Map[String, Set[String]]().withDefaultValue(Set())

  private val sseEntries = mutable.Map[String, Entry]()

  def register(streamId: String, ref: ActorRef, name: String): Unit = {
    sseEntries(streamId) = Entry(streamId, ref, name, System.currentTimeMillis())
  }

  def subscribe(streamId: String, expressionId: String): Unit = synchronized {
    val entry = sseEntries.get(streamId)
    if (entry.nonEmpty)
      expressionIdToActorRef(expressionId) += entry.get.actorRef
    streamIdToExpressionId(streamId) += expressionId
  }

  def unsubscribe(streamId: String, expressionId: String): Unit = synchronized {
    val entry = sseEntries.get(streamId)
    if (entry.nonEmpty)
      expressionIdToActorRef(expressionId) -= entry.get.actorRef
    streamIdToExpressionId(streamId) -= expressionId
  }

  def unsubscribeAll(streamId: String): Unit = synchronized {
    val entry = sseEntries.get(streamId)
    if (entry.nonEmpty)
      expressionIdToActorRef.keySet.foreach(id => expressionIdToActorRef(id) -= entry.get.actorRef)
    streamIdToExpressionId.remove(streamId)
  }

  def getActorsForExpressionId(expressionId: String): Set[ActorRef] = synchronized {
    expressionIdToActorRef(expressionId)
  }

  def getExpressionsForStreamId(streamId: String): Set[String] = synchronized {
    streamIdToExpressionId(streamId)
  }

  def entries: List[Entry] = synchronized { sseEntries.values.toList }

  override def getAllExpressions: Map[String, Set[String]] = synchronized {
    streamIdToExpressionId.toMap
  }
}
