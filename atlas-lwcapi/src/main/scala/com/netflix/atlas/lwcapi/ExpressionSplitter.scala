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

import com.netflix.atlas.core.model.{ModelExtractors, Query, StyleVocabulary}
import com.netflix.atlas.core.stacklang.Interpreter
import ExpressionSplitter._
import com.netflix.atlas.core.model.Query.KeyQuery

case class ExpressionSyntaxException(message: String) extends IllegalArgumentException(message)

class ExpressionSplitter (val interner: QueryInterner) {
  private var interpreter = Interpreter(StyleVocabulary.allWords)

  private def intern(query: Query): Query = {
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
        interner.getOrElseUpdate(q, Query.And(intern(q.q1), intern(q.q2)))
      case q: Query.Or =>
        interner.getOrElseUpdate(q, Query.Or(intern(q.q1), intern(q.q2)))
      case q: Query.Not =>
        interner.getOrElseUpdate(q, Query.Not(intern(q.q)))
    }
  }

  def split(s: String): Option[QueryContainer] = synchronized {
    try {
      val context = interpreter.execute(s)
      val queries = context.stack.collect {
        case ModelExtractors.PresentationType(t) =>
          t.expr.dataExprs.map(e => intern(compress(e.query)))
      }
      val dataExpressions = context.stack.collect {
        case ModelExtractors.PresentationType(t) =>
          t.expr.dataExprs.map(e => e.toString)
      }
      if (dataExpressions.isEmpty) {
        None
      } else {
        Some(QueryContainer(queries.flatten.distinct, dataExpressions.flatten.distinct))
      }
    } catch {
      case _: Exception => None
    }
  }

  private val keys = Set("nf.app", "nf.stack", "nf.cluster", "nf.zone", "nf.node")
  private def simplify(query: Query): Query = {
    val newQuery = query match {
      case Query.And(Query.True, q)  => simplify(q)
      case Query.And(q, Query.True)  => simplify(q)
      case Query.Or(Query.True, q)   => Query.True
      case Query.Or(q, Query.True)   => Query.True
      case Query.And(Query.False, q) => Query.False
      case Query.And(q, Query.False) => Query.False
      case Query.Or(Query.False, q)  => simplify(q)
      case Query.Or(q, Query.False)  => simplify(q)
      case Query.And(q1, q2)         => Query.And(simplify(q1), simplify(q2))
      case Query.Or(q1, q2)          => Query.Or(simplify(q1), simplify(q2))
      case Query.Not(Query.True)     => Query.True // Not(True) needs to remain True
      case Query.Not(Query.False)    => Query.True
      case Query.Not(q)              => Query.Not(simplify(q))
      case q                         => q
    }
    if (newQuery != query) simplify(newQuery) else newQuery
  }

  def compress(expr: Query): Query = {
    val tmp = expr.rewrite { case kq: KeyQuery if !keys.contains(kq.k) => Query.True }
    simplify(tmp.asInstanceOf[Query])
  }
}

object ExpressionSplitter {
  type QueryInterner = scala.collection.mutable.AnyRefMap[Query, Query]

  case class QueryContainer(matchExprs: List[Query], dataExprs: List[String])

  def apply(interner: QueryInterner) = new ExpressionSplitter(interner)
}
