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

import akka.actor.{ActorLogging, ActorRef}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable

case class SubscriptionManagerImpl() extends SubscriptionManager {
  import SubscriptionManager._

  private val exprToStream = mutable.Map[String, Set[String]]().withDefaultValue(Set())
  private val streamToExpr = mutable.Map[String, Set[String]]().withDefaultValue(Set())
  private val streamToEntry = mutable.Map[String, Entry]()
  private val exprToSplit = ExpressionDatabaseImpl()

  def register(streamId: String, ref: ActorRef, name: String): Unit = synchronized {
    streamToEntry(streamId) = Entry(streamId, ref, name, System.currentTimeMillis())
  }

  def subscribe(streamId: String, expressionId: String): Unit = synchronized {
    streamToExpr(streamId) += expressionId
    exprToStream(expressionId) += streamId
  }

  def unsubscribe(streamId: String, expressionId: String): Unit = synchronized {
    streamToExpr(streamId) -= expressionId
    exprToStream(expressionId) -= streamId
  }

  def unsubscribeAll(streamId: String): List[String] = synchronized {
    val ids = streamToExpr.remove(streamId)
    if (ids.isDefined) {
      ids.get.foreach(k => exprToStream(k) -= streamId)
      ids.get.toList
    } else {
      List()
    }
  }

  def actorsForExpression(expressionId: String): Set[ActorRef] = synchronized {
    exprToStream.getOrElse(expressionId, Set()).map(streamId => streamToEntry(streamId).actorRef)
  }

  override def subscribersForExpression(expressionId: String): Set[String] = {
    exprToStream.getOrElse(expressionId, Set())
  }
}
