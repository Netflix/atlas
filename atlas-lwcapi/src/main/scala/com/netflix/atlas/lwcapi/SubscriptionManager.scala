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

class SubscriptionManager() {
  import SubscriptionManager._

  private val exprToStream = mutable.Map[String, Set[String]]().withDefaultValue(Set())
  private val streamToExpr = mutable.Map[String, Set[String]]().withDefaultValue(Set())
  private val streamToEntry = mutable.Map[String, Entry]()

  def register(streamId: String, ref: ActorRef, name: String): Unit = synchronized {
    streamToEntry(streamId) = Entry(streamId, ref, name, System.currentTimeMillis())
  }

  def unregister(streamId: String): List[String] = synchronized {
    streamToEntry.remove(streamId)
    val ids = streamToExpr.remove(streamId)
    if (ids.isDefined) {
      ids.get.foreach(k => exprToStream(k) -= streamId)
      ids.get.toList
    } else {
      List()
    }
  }

  def registration(streamId: String): Option[Entry] = synchronized {
    streamToEntry.get(streamId)
  }

  def subscribe(streamId: String, expressionId: String): Unit = synchronized {
    streamToExpr(streamId) += expressionId
    exprToStream(expressionId) += streamId
  }

  def unsubscribe(streamId: String, expressionId: String): Unit = synchronized {
    streamToExpr(streamId) -= expressionId
    exprToStream(expressionId) -= streamId
  }

  def actorsForExpression(expressionId: String): Set[ActorRef] = synchronized {
    exprToStream.getOrElse(expressionId, Set()).flatMap(streamId => streamToEntry.get(streamId)).map(e => e.actorRef)
  }

  def subscribersForExpression(expressionId: String): Set[String] = synchronized {
    exprToStream.getOrElse(expressionId, Set())
  }

  def expressionsForSubscriber(streamId: String): Set[String] = synchronized {
    streamToExpr.getOrElse(streamId, Set())
  }
}

object SubscriptionManager {
  case class Entry(streamId: String, actorRef: ActorRef, name: String, connectTime: Long)
}
