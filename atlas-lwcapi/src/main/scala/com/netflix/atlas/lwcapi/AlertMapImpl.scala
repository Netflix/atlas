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
import com.netflix.atlas.lwcapi.ExpressionSplitter.SplitResult
import com.netflix.frigga.Names

import scala.collection.mutable
import scala.collection.JavaConverters._

case class AlertMapImpl() extends AlertMap {
  import AlertMap._

  private val knownExpressions = new ConcurrentHashMap[String, SplitResult]().asScala
  private var queryIndex = QueryIndex.create[(String, SplitResult)](Nil)

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

  def addExpr(split: SplitResult): Unit = {
    // Only replace the object if it is not there, to avoid keeping many identical objects around.
    val replaced = knownExpressions.putIfAbsent(split.id, split)
    queryListChanged = replaced.isEmpty
    if (testMode)
      regenerateQueryIndex()
  }

  def delExpr(split: SplitResult): Unit = {
    val removed = knownExpressions.remove(split.id)
    queryListChanged = removed.isDefined
    if (testMode)
      regenerateQueryIndex()
  }

  def expressionsForCluster(cluster: String): List[ReturnableExpression] = {
    val name = Names.parseName(cluster)
    var tags = Map("nf.cluster" -> name.getCluster)
    if (name.getApp != null)
      tags = tags + ("nf.app" -> name.getApp)
    if (name.getStack != null)
      tags = tags + ("nf.stack" -> name.getStack)
    val matches = queryIndex.matchingEntries(tags)
    val matchingDataExpressions = mutable.Map[String, Boolean]()
    val matchingDataItems = mutable.Map[SplitResult, Boolean]()
    matches.foreach(m => {
      matchingDataExpressions(m._1) = true
      matchingDataItems(m._2) = true
    })

    val ret = matchingDataItems.map {case (item, flag) =>
      val dataExprs = item.split.map(x => x.dataExpr).map(dataexpr =>
        if (matchingDataExpressions.contains(dataexpr)) dataexpr else ""
      )
      ReturnableExpression(item.id, item.frequency, dataExprs)
    }
    ret.toList.distinct
  }

  private def regenerateQueryIndex(): Unit = {
    queryListChanged = false
    val map = knownExpressions.flatMap { case (exprKey, data) =>
      data.split.map(container =>
        QueryIndex.Entry(container.matchExpr, (container.dataExpr, data))
      )
    }.toList
    queryIndex = QueryIndex.create(map)
  }
}
