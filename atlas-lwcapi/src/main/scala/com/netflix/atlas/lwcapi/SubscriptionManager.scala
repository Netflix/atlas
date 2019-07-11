/*
 * Copyright 2014-2019 Netflix, Inc.
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
import com.typesafe.scalalogging.StrictLogging

import scala.jdk.CollectionConverters._

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
class SubscriptionManager[T] extends StrictLogging {

  import SubscriptionManager._

  private val registrations = new ConcurrentHashMap[String, StreamInfo[T]]()

  private val subHandlers = new ConcurrentHashMap[String, ConcurrentSet[T]]()

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
    * Add handler that should receive data for a given subscription. The update is synchronized
    * to coordinate with the deletion of the handlers set when it is empty. Reads will just
    * access the concurrent map without synchronization. Returns true if a new handler was added.
    */
  private def addHandler(subId: String, handler: T): Boolean = {
    subHandlers.synchronized {
      val handlers = subHandlers.computeIfAbsent(subId, _ => new ConcurrentSet[T])
      val added = handlers.add(handler)
      if (added) {
        logger.debug(s"added handler for $subId: $handler")
      }
      added
    }
  }

  /**
    * Remove a handler from the set that should receive data for a given subscription. The
    * is empty check and removal are synchronized to coordinate with the updates adding
    * new handlers to the set.
    */
  private def removeHandler(subId: String, handler: T): Boolean = {
    val handlers = subHandlers.get(subId)
    (handlers != null) && {
      val removed = handlers.remove(handler)
      if (removed) {
        logger.debug(s"removed handler for $subId: $handler")
      }
      subHandlers.synchronized {
        if (handlers.isEmpty) {
          logger.debug(s"removing $subId, no more active handlers")
          subHandlers.remove(subId)
        }
      }
      removed
    }
  }

  /**
    * Register a new stream with the provided id. The `handler` is used by the caller to
    * interact with the stream. The caller can use [[handlersForSubscription()]] to get a
    * list of handlers that should be called for a given subscription.
    *
    * Returns true if it is a new registration.
    */
  def register(streamId: String, handler: T): Boolean = {
    val registered = registrations.putIfAbsent(streamId, new StreamInfo[T](handler)) == null
    if (registered) {
      logger.debug(s"registered $streamId")
    }
    registered
  }

  /**
    * Cleanup all resources associated with the provided stream id. If it is the last stream
    * associated with a particular subscription, then the subscription will automatically be
    * removed.
    */
  def unregister(streamId: String): Option[T] = {
    val result = Option(registrations.remove(streamId)).map { info =>
      logger.debug(s"unregistered $streamId")
      info.subscriptions.foreach { sub =>
        removeHandler(sub.metadata.id, info.handler)
      }
      info.handler
    }
    queryListChanged = true
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
  private[lwcapi] def subscribe(streamId: String, sub: Subscription): T = {
    subscribe(streamId, List(sub))._1
  }

  /**
    * Start sending data for the subscription to the given stream id. Returns the handler
    * along with a list of newly added subscriptions.
    */
  def subscribe(streamId: String, subs: List[Subscription]): (T, List[Subscription]) = {
    logger.debug(s"updating subscriptions for $streamId")
    val info = getInfo(streamId)
    val addedSubs = List.newBuilder[Subscription]
    subs.foreach { sub =>
      if (info.subs.putIfAbsent(sub.metadata.id, sub) == null) {
        logger.debug(s"subscribed $streamId to $sub")
        addedSubs += sub
      }
      queryListChanged |= addHandler(sub.metadata.id, info.handler)
    }
    info.handler -> addedSubs.result()
  }

  /**
    * Stop sending data for the subscription to the given stream id.
    */
  def unsubscribe(streamId: String, subId: String): Unit = {
    val info = getInfo(streamId)
    if (info.subs.remove(subId) != null) {
      logger.debug(s"unsubscribed $streamId from $subId")
    }
    queryListChanged = removeHandler(subId, info.handler)
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
    if (vs == null) Nil else vs.values
  }

  def clear(): Unit = {
    logger.debug("clearing all subscriptions")
    registrations.clear()
    queryListChanged = true
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

  class ConcurrentSet[T] {
    private val data = new ConcurrentHashMap[T, T]()

    def add(value: T): Boolean = {
      data.put(value, value) == null
    }

    def remove(value: T): Boolean = {
      data.remove(value) != null
    }

    def isEmpty: Boolean = {
      data.isEmpty
    }

    def values: List[T] = {
      import scala.jdk.CollectionConverters._
      data.values().asScala.toList
    }
  }
}
