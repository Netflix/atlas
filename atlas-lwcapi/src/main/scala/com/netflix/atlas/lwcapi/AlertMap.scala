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

case class AlertMap() {
  import AlertMap._

  private val knownExpressions = scala.collection.mutable.Map.empty[String, Set[DataItem]]

  private var queryIndex = QueryIndex(Nil)
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
    val ret = scala.collection.mutable.ListBuffer.empty[ReturnableExpression]
    for ((expr, data) <- knownExpressions) {
      data.foreach(item => {
        ret += ReturnableExpression(expr, item.frequency, item.expressionContainer.dataExprs)
      })
    }
    ret.toList
  }

  private def regenerateQueryIndex() = {
    // todo: regenerate the actual index
  }
}

object AlertMap {
  case class DataItem(frequency: Long, expressionContainer: ExpressionSplitter.QueryContainer)
  case class ReturnableExpression(expression: String, frequency: Long, dataExpressions: List[String])

  lazy val globalAlertMap = new AlertMap()
}
