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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

import com.netflix.atlas.core.index.QueryIndex
import com.netflix.frigga.Names

import scala.collection.JavaConverters._

/**
  * Manages the set of streams and associated subscriptions. There are two basic concepts:
  *
  * - register/unregister: informs the manager about a stream and the associated handler. The
  *   handler is just an object that can be used by the caller to interact with whatever is
  *   responsible for processing the results. A common example is an ActorRef.
  *
  * - subscribe/unsubscribe: informs the manager that a given stream should receive or stop
  *   receiving data for a given expression.
  */
class SubscriptionManager[T] {

  import SubscriptionManager._

  private val registrations = new ConcurrentHashMap[String, StreamInfo[T]]()

  private val subHandlers = new ConcurrentHashMap[String, List[T]]()

  @volatile private var queryIndex = QueryIndex.create[Subscription](Nil)
  @volatile private var queryListChanged = false

  // Background process for updating the query index. It is not done inline because rebuilding
  // the index can be computationally expensive.
  private val ex = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("ExpressionDatabase"))
  ex.scheduleWithFixedDelay(() => regenerateQueryIndex(), 1, 1, TimeUnit.SECONDS)

  /** Rebuild the query index if there have been changes since it was last created. */
  private[lwcapi] def regenerateQueryIndex(): Unit = {
    if (queryListChanged) {
      queryListChanged = false
      val entries = subscriptions.map { sub =>
        QueryIndex.Entry(sub.query, sub)
      }
      queryIndex = QueryIndex.create(entries)
    }
  }

  /**
    * Refresh the map from a subscription to a set of handlers. This is precomputed on
    * each update so the [[handlersForSubscription()]] call will be as cheap as possible.
    */
  private def updateSubHandlers(): Unit = synchronized {
    val handlers = registrations
      .values()
      .asScala
      .flatMap { info =>
        info.subscriptions.map(_.metadata.id -> info.handler)
      }
      .toList
      .groupBy(_._1)
      .map(t => t._1 -> t._2.map(_._2))

    // Copy current values into concurrent map
    handlers.foreach { h =>
      subHandlers.put(h._1, h._2)
    }

    // Remove any ids that are no longer present
    val keys = subHandlers.keySet().asScala.toSet
    val removed = keys -- handlers.keySet
    removed.foreach { k =>
      subHandlers.remove(k)
    }

    queryListChanged = true
  }

  /**
    * Register a new stream with the provided id. The `handler` is used by the caller to
    * interact with the stream. The caller can use [[handlersForSubscription()]] to get a
    * list of handlers that should be called for a given subscription.
    */
  def register(streamId: String, handler: T): Unit = synchronized {
    registrations.put(streamId, new StreamInfo[T](handler))
    updateSubHandlers()
  }

  /**
    * Cleanup all resources associated with the provided stream id. If it is the last stream
    * associated with a particular subscription, then the subscription will automatically be
    * removed.
    */
  def unregister(streamId: String): Option[T] = {
    val result = Option(registrations.remove(streamId)).map(_.handler)
    updateSubHandlers()
    result
  }

  private def getInfo(streamId: String): StreamInfo[T] = {
    val info = registrations.get(streamId)
    if (info == null) {
      throw new IllegalStateException(s"stream with id '$streamId' has not been registered")
    }
    info
  }

  /**
    * Start sending data for the subscription to the given stream id.
    */
  def subscribe(streamId: String, sub: Subscription): T = synchronized {
    subscribe(streamId, List(sub))
  }

  /**
    * Start sending data for the subscription to the given stream id.
    */
  def subscribe(streamId: String, subs: List[Subscription]): T = synchronized {
    val info = getInfo(streamId)
    subs.foreach { sub =>
      info.subs.put(sub.metadata.id, sub)
    }
    updateSubHandlers()
    info.handler
  }

  /**
    * Stop sending data for the subscription to the given stream id.
    */
  def unsubscribe(streamId: String, subId: String): Unit = synchronized {
    getInfo(streamId).subs.remove(subId)
    updateSubHandlers()
  }

  /**
    * Return the set of all current subscriptions across all streams.
    */
  def subscriptions: List[Subscription] = {
    registrations
      .values()
      .asScala
      .flatMap(_.subscriptions)
      .toList
      .distinct
  }

  /**
    * Return the set of subscriptions that can potentially match the provided cluster. This
    * is typically used by clients running as part of a cluster to limit the set of expressions
    * that are checked locally on the node.
    */
  def subscriptionsForCluster(cluster: String): List[Subscription] = {
    val name = Names.parseName(cluster)
    val tags = Map.newBuilder[String, String]
    tags += ("nf.cluster" -> name.getCluster)
    if (name.getApp != null)
      tags += ("nf.app" -> name.getApp)
    if (name.getStack != null)
      tags += ("nf.stack" -> name.getStack)
    queryIndex.matchingEntries(tags.result)
  }

  /**
    * Return all of the subscriptions that are in use for a given stream.
    */
  def subscriptionsForStream(streamId: String): List[Subscription] = {
    getInfo(streamId).subscriptions
  }

  /**
    * Return the set of handlers that should receive data for a given subscription id. This
    * method should be assumed to be on a hot path that will be called regularly as data is
    * flowing through.
    */
  def handlersForSubscription(subId: String): List[T] = {
    val vs = subHandlers.get(subId)
    if (vs == null) Nil else vs
  }

  def clear(): Unit = {
    registrations.clear()
    updateSubHandlers()
    regenerateQueryIndex()
  }
}

object SubscriptionManager {

  class StreamInfo[T](
    val handler: T,
    val subs: ConcurrentHashMap[String, Subscription] =
      new ConcurrentHashMap[String, Subscription]()
  ) {

    def subscriptions: List[Subscription] = {
      subs.values().asScala.toList
    }
  }
}
