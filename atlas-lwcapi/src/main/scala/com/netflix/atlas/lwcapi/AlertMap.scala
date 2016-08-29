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
import com.netflix.atlas.core.model.{ModelExtractors, Query, StyleVocabulary}
import com.netflix.atlas.core.stacklang.Interpreter

import scala.collection.immutable.TreeMap

case class AlertMap() {
  type QueryInterner = scala.collection.mutable.AnyRefMap[Query, Query]

  private val splitter = ExpressionSplitter()
  private val knownExpressions = scala.collection.mutable.Map[ExpressionWithFrequency, Set[ExpressionWithFrequency]]()
  private val interpreter = Interpreter(StyleVocabulary.allWords)

  private var queryIndex = QueryIndex(Nil)
  private var interner: QueryInterner = _

  def addExpr(expression: ExpressionWithFrequency): Unit = {
    val dataExpressions = splitter.split(expression)
    synchronized {
      if (!knownExpressions.contains(expression)) {
        knownExpressions(expression) = dataExpressions
        regenerateQueryIndex()
      }
    }
  }

  def delExpr(expression: ExpressionWithFrequency): Unit = synchronized {
    val perhapsRemoved = knownExpressions.remove(expression)
    if (perhapsRemoved.isDefined) {
      regenerateQueryIndex()
    }
  }

  def expressionsForCluster(cluster: String) = synchronized {
    val ret = scala.collection.mutable.Map[ExpressionWithFrequency, Set[ExpressionWithFrequency]]()
    val matchingQueries = queryIndex.matchingEntries(Map("nf.cluster" -> "skan"))
    matchingQueries.foreach(item =>
      val expression = ExpressionWithFrequency(item, )
      Map("expression" -> item, "data" -> knownExpressions(item.toString)))
    knownExpressions.keySet.toSet
  }

  private def regenerateQueryIndex() = {
    interner = new QueryInterner // TODO: Do we want to flush the interner cache each time?
    val queries = knownExpressions.keySet.map(item => parse(interner, item.expression))
    val list = queries.flatten.toList.map(item => item.)
    queryIndex = QueryIndex(list)
  }

  private def intern(interner: QueryInterner, query: Query): Query = {
    query match {
      case Query.True =>
        query
      case Query.False =>
        query
      case q: Query.Equal =>
        interner.getOrElseUpdate(q, Query.Equal(q.k.intern(), q.v.intern()))
      case q: Query.LessThan =>
        interner.getOrElseUpdate(q, Query.LessThan(q.k.intern(), q.v.intern()))
      case q: Query.LessThanEqual =>
        interner.getOrElseUpdate(q, Query.LessThanEqual(q.k.intern(), q.v.intern()))
      case q: Query.GreaterThan =>
        interner.getOrElseUpdate(q, Query.GreaterThan(q.k.intern(), q.v.intern()))
      case q: Query.GreaterThanEqual =>
        interner.getOrElseUpdate(q, Query.GreaterThanEqual(q.k.intern(), q.v.intern()))
      case q: Query.Regex =>
        interner.getOrElseUpdate(q, Query.Regex(q.k.intern(), q.v.intern()))
      case q: Query.RegexIgnoreCase =>
        interner.getOrElseUpdate(q, Query.RegexIgnoreCase(q.k.intern(), q.v.intern()))
      case q: Query.In =>
        interner.getOrElseUpdate(q, Query.In(q.k.intern(), q.vs.map(_.intern())))
      case q: Query.HasKey =>
        interner.getOrElseUpdate(q, Query.HasKey(q.k.intern()))
      case q: Query.And =>
        interner.getOrElseUpdate(q, Query.And(intern(interner, q.q1), intern(interner, q.q2)))
      case q: Query.Or =>
        interner.getOrElseUpdate(q, Query.Or(intern(interner, q.q1), intern(interner, q.q2)))
      case q: Query.Not =>
        interner.getOrElseUpdate(q, Query.Not(intern(interner, q.q)))
    }
  }

  private def parse(interner: QueryInterner, s: String): List[Query] = {
    try {
      val queries = interpreter.execute(s).stack.collect {
        case ModelExtractors.PresentationType(t) =>
          t.expr.dataExprs.map(e => intern(interner, e.query))
      }
      queries.flatten.distinct
    } catch {
      case _: Exception => Nil
    }
  }

}

object AlertMap {
  lazy val globalAlertMap = new AlertMap()
}
