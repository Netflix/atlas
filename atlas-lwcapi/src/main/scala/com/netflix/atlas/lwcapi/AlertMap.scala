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
import com.netflix.frigga.Names

case class AlertMap() {
  import AlertMap._

  private val knownExpressions = scala.collection.mutable.Map.empty[String, Set[DataItem]]

  private var queryIndex = QueryIndex.create[String](Nil)
  private var interner = new ExpressionSplitter.QueryInterner()

  def addExpr(expression: ExpressionWithFrequency): Unit = {
    val splitter = ExpressionSplitter(interner)
    val dataExpressions = splitter.split(expression.expression)
    if (dataExpressions.isDefined) {
      synchronized {
        if (knownExpressions.contains(expression.expression)) {
          knownExpressions(expression.expression) += DataItem(expression.frequency, dataExpressions.get)
        } else {
          knownExpressions(expression.expression) = Set(DataItem(expression.frequency, dataExpressions.get))
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
    val ret = scala.collection.mutable.ListBuffer.empty[ReturnableExpression]
    var tags = Map("nf.cluster" -> name.getCluster)
    if (name.getApp != null)
      tags = tags + ("nf.app" -> name.getApp)
    if (name.getStack != null)
      tags = tags + ("nf.stack" -> name.getStack)
    println(tags)
    for ((expr, data) <- knownExpressions) {
      data.foreach(item => {
        item.expressionContainer.matchExprs.head.matches()
        ret += ReturnableExpression(expr, item.frequency, item.expressionContainer.dataExprs)
      })
    }
    ret.toList
  }

  private def regenerateQueryIndex() = {
    for ((expr, data) <- knownExpressions) {
      data.foreach(item => {
        // todo: regenerate the actual index
      })
    }
  }
}

object AlertMap {
  case class DataItem(frequency: Long, expressionContainer: ExpressionSplitter.QueryContainer)
  case class ReturnableExpression(expression: String, frequency: Long, dataExpressions: List[String])

  lazy val globalAlertMap = new AlertMap()
}
