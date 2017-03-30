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
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit

import com.netflix.atlas.core.index.QueryIndex
import com.netflix.atlas.core.model.Query
import com.netflix.frigga.Names
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._

class NamedThreadFactory(name: String) extends ThreadFactory {
  def newThread(r: Runnable): Thread = {
    val thread = new Thread(r, name)
    thread.setDaemon(true)
    thread
  }
}

class ExpressionDatabase() extends StrictLogging {
  import ExpressionDatabase._

  private val knownExpressions = new ConcurrentHashMap[String, Item]().asScala
  private var queryIndex = QueryIndex.create[ExpressionWithFrequency](Nil)

  @volatile private var queryListChanged = false

  private val ex = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("ExpressionDatabase"))
  private val task = new Runnable {
    def run(): Unit = {
      regenerateQueryIndex()
    }
  }
  ex.scheduleWithFixedDelay(task, 1, 1, TimeUnit.SECONDS)

  def addExpr(expr: ExpressionWithFrequency, query: Query): Boolean = {
    // Only replace the object if it is not there, to avoid keeping many identical objects around.
    val replaced = knownExpressions.putIfAbsent(expr.id, Item(query, expr))
    val changed = replaced.isEmpty
    queryListChanged ||= changed
    changed
  }

  def delExpr(id: String): Boolean = {
    val removed = knownExpressions.remove(id)
    val changed = removed.isDefined
    queryListChanged ||= changed
    changed
  }

  def clear(): Unit = {
    knownExpressions.clear()
    queryListChanged = true
  }

  def hasExpr(id: String): Boolean = knownExpressions.contains(id)

  def expr(id: String): Option[ExpressionWithFrequency] = {
    knownExpressions.get(id).map(_.expr)
  }

  def expressions: List[ExpressionWithFrequency] = {
    knownExpressions.values.map(_.expr).toList
  }

  def expressionsForCluster(cluster: String): List[ExpressionWithFrequency] = {
    val name = Names.parseName(cluster)
    val tags = Map.newBuilder[String, String]
    tags += ("nf.cluster" -> name.getCluster)
    if (name.getApp != null)
      tags += ("nf.app" -> name.getApp)
    if (name.getStack != null)
      tags += ("nf.stack" -> name.getStack)
    queryIndex.matchingEntries(tags.result)
  }

  private[lwcapi] def regenerateQueryIndex(): Unit = {
    if (queryListChanged) {
      queryListChanged = false
      val entries = knownExpressions.toList.map { case (_, item) =>
        QueryIndex.Entry(item.queries, item.expr)
      }
      logger.debug(s"Regenerating QueryIndex with ${entries.size} entries")
      queryIndex = QueryIndex.create(entries)
    }
  }
}

object ExpressionDatabase {
  case class Item(queries: Query, expr: ExpressionWithFrequency)
}
