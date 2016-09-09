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

import com.netflix.atlas.core.index.QueryIndex
import com.netflix.atlas.core.model.Query
import com.netflix.frigga.Names

import scala.collection.mutable

case class AlertMap() {
  import AlertMap._

  private val knownExpressions = scala.collection.mutable.Map.empty[String, Set[DataItem]]

  private var queryIndex = QueryIndex.create[(String, String)](Nil)
  private var interner = new ExpressionSplitter.QueryInterner()

  def addExpr(expression: ExpressionWithFrequency): Unit = {
    val splitter = ExpressionSplitter(interner)
    val dataExpressions = splitter.split(expression.expression)
    if (dataExpressions.nonEmpty) {
      synchronized {
        if (knownExpressions.contains(expression.expression)) {
          knownExpressions(expression.expression) += DataItem(expression.frequency, dataExpressions)
        } else {
          knownExpressions(expression.expression) = Set(DataItem(expression.frequency, dataExpressions))
          regenerateQueryIndex()
        }
      }
    }
  }

  def delExpr(expression: ExpressionWithFrequency): Unit = synchronized {
    val perhapsRemoved = knownExpressions.remove(expression.expression)
    if (perhapsRemoved.isDefined) {
      regenerateQueryIndex()
    }
  }

  def expressionsForCluster(cluster: String): List[ReturnableExpression] = synchronized {
    val name = Names.parseName(cluster)
    var tags = Map("nf.cluster" -> name.getCluster)
    if (name.getApp != null)
      tags = tags + ("nf.app" -> name.getApp)
    if (name.getStack != null)
      tags = tags + ("nf.stack" -> name.getStack)
    val matches = queryIndex.matchingEntries(tags)
    val matchingExpressions = matches.map(m => m._1)
    val matchingDataExpressions = mutable.Map[String, Boolean]()
    matches.foreach(m => matchingDataExpressions(m._2) = true)

    val ret = matchingExpressions.flatMap(s => {
      val data = knownExpressions(s)
      data.map(item => {
        val dataExprs = item.containers.map(x => x.dataExpr).filter(x => matchingDataExpressions.contains(x))
        ReturnableExpression(s, item.frequency, dataExprs)
      })
    })
    ret.distinct
  }

  private def regenerateQueryIndex() = {
    val map = knownExpressions.flatMap {case (expr, data) =>
      data.map(item =>
        item.containers.map(container =>
          QueryIndex.Entry(container.matchExpr, (expr, container.dataExpr))
        )
      )
    }.flatten.toList
    queryIndex = QueryIndex.create(map)
  }
}

object AlertMap {
  case class DataItem(frequency: Long, containers: List[ExpressionSplitter.QueryContainer])
  case class ReturnableExpression(expression: String, frequency: Long, dataExpressions: List[String])

  lazy val globalAlertMap = new AlertMap()
}
