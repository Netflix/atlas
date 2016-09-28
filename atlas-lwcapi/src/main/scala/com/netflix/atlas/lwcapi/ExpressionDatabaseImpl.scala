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

import java.util.concurrent.{ConcurrentHashMap, ScheduledThreadPoolExecutor, TimeUnit}

import com.netflix.atlas.core.index.QueryIndex
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.lwcapi.ExpressionSplitter.SplitResult
import com.netflix.frigga.Names
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.collection.JavaConverters._

case class ExpressionDatabaseImpl() extends ExpressionDatabase with StrictLogging {
  case class Item(queries: Query, expr: ExpressionWithFrequency)

  private val knownExpressions = new ConcurrentHashMap[String, Item]().asScala
  private var queryIndex = QueryIndex.create[ExpressionWithFrequency](Nil)

  private var queryListChanged @volatile = false
  private var testMode = false

  val ex = new ScheduledThreadPoolExecutor(1)
  val task = new Runnable {
    def run() = {
      if (queryListChanged) {
        regenerateQueryIndex()
      }
    }
  }
  val f = ex.scheduleAtFixedRate(task, 1, 1, TimeUnit.SECONDS)

  def setTestMode() = { testMode = true }

  def addExpr(expr: ExpressionWithFrequency, queries: Query): Boolean = {
    // Only replace the object if it is not there, to avoid keeping many identical objects around.
    val replaced = knownExpressions.putIfAbsent(expr.id, Item(queries, expr))
    val changed = replaced.isEmpty
    queryListChanged |= changed
    if (testMode)
      regenerateQueryIndex()
    changed
  }

  def delExpr(id: String): Boolean = {
    val removed = knownExpressions.remove(id)
    val changed = removed.isDefined
    queryListChanged |= changed
    if (testMode)
      regenerateQueryIndex()
    changed
  }

  override def hasExpr(id: String): Boolean = knownExpressions.contains(id)

  override def expr(id: String): Option[ExpressionWithFrequency] = {
    val ret = knownExpressions.get(id)
    if (ret.isDefined) Some(ret.get.expr) else None
  }

  def expressionsForCluster(cluster: String): List[ExpressionWithFrequency] = {
    val name = Names.parseName(cluster)
    var tags = Map("nf.cluster" -> name.getCluster)
    if (name.getApp != null)
      tags = tags + ("nf.app" -> name.getApp)
    if (name.getStack != null)
      tags = tags + ("nf.stack" -> name.getStack)
    val matches = queryIndex.matchingEntries(tags)
    matches.distinct
  }

  private def regenerateQueryIndex(): Unit = {
    queryListChanged = false
    val map = knownExpressions.map { case (query, item) =>
      QueryIndex.Entry(item.queries, item.expr)
    }.toList
    logger.debug(s"Regenerating QueryIndex with ${map.size} entries")
    queryIndex = QueryIndex.create(map)
  }
}
