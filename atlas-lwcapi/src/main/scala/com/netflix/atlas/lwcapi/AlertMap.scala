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

case class AlertMap() {
  private val splitter = ExpressionSplitter()
  private val dataExprs = scala.collection.mutable.Map[Set[ExpressionWithFrequency], Set[ExpressionWithFrequency]]()

  def addExpr(expression: ExpressionWithFrequency): Unit = {
    val dataExpressions = splitter.split(expression)
    synchronized {
      if (dataExprs.keySet.contains(dataExpressions)) {
        val currentSet = dataExprs(dataExpressions)
        dataExprs(dataExpressions) = currentSet + expression
      } else {
        dataExprs(dataExpressions) = Set(expression)
      }
    }
  }

  def delExpr(expression: ExpressionWithFrequency): Unit = {
    val dataExpressions = splitter.split(expression)
    synchronized {
      if (dataExprs.keySet.contains(dataExpressions)) {
        val currentSet = dataExprs(dataExpressions)
        val newSet = currentSet - expression
        if (newSet.nonEmpty) {
          dataExprs(dataExpressions) = newSet
        } else {
          dataExprs -= dataExpressions
        }
      }
    }
  }

  def exprsForDataExprSet(dataExprSet: Set[ExpressionWithFrequency]) : Set[ExpressionWithFrequency] = synchronized {
    if (dataExprs.keySet.contains(dataExprSet))
      dataExprs(dataExprSet)
    else
      Set()
  }

  def allDataExpressions(): Set[Set[ExpressionWithFrequency]] = synchronized {
    dataExprs.keySet.toSet
  }

  def dataExpressionsForCluster(cluster: String) = synchronized {
    // XXXMLG TODO keep track of expressions by cluster
    dataExprs.keySet.toSet
  }
}

object AlertMap {
  lazy val globalAlertMap = new AlertMap()
}
