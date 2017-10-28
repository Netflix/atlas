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
import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.CacheLoader
import com.github.benmanes.caffeine.cache.Caffeine
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.Query.KeyQuery
import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.core.stacklang.Interpreter

import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Splits a complete graph expression (StyleExpr) string into a set of subscriptions. Each
  * subscription is based on the underlying data expressions (DataExpr) that get pushed back
  * to the systems supplying data to LWCAPI.
  */
class ExpressionSplitter {

  import ExpressionSplitter._

  private val keepKeys = Set("nf.app", "nf.stack", "nf.cluster")

  private val interpreter = Interpreter(StyleVocabulary.allWords)

  /**
    * Processing the expressions can be quite expensive. In particular compiling regular
    * expressions to ensure they are valid. Generally the set of expressions should not
    * vary much over time and the evaluator library will regularly submit the full list
    * to sync with. This cache prevents the reprocessing for expressions that have already
    * been seen recently.
    */
  private val exprCache = {
    val loader = new CacheLoader[String, Try[List[DataExprMeta]]] {
      def load(expr: String): Try[List[DataExprMeta]] = parse(expr)
    }
    Caffeine
      .newBuilder()
      .expireAfterAccess(10, TimeUnit.MINUTES)
      .build(loader)
  }

  // TODO: https://github.com/Netflix/atlas/issues/729
  private val interner = new ConcurrentHashMap[Query, Query]()

  private[lwcapi] def intern(query: Query): Query = {
    query match {
      case Query.True =>
        query
      case Query.False =>
        query
      case q: Query.Equal =>
        interner.computeIfAbsent(q, _ => Query.Equal(q.k.intern(), q.v.intern()))
      case q: Query.LessThan =>
        interner.computeIfAbsent(q, _ => Query.LessThan(q.k.intern(), q.v.intern()))
      case q: Query.LessThanEqual =>
        interner.computeIfAbsent(q, _ => Query.LessThanEqual(q.k.intern(), q.v.intern()))
      case q: Query.GreaterThan =>
        interner.computeIfAbsent(q, _ => Query.GreaterThan(q.k.intern(), q.v.intern()))
      case q: Query.GreaterThanEqual =>
        interner.computeIfAbsent(q, _ => Query.GreaterThanEqual(q.k.intern(), q.v.intern()))
      case q: Query.Regex =>
        interner.computeIfAbsent(q, _ => Query.Regex(q.k.intern(), q.v.intern()))
      case q: Query.RegexIgnoreCase =>
        interner.computeIfAbsent(q, _ => Query.RegexIgnoreCase(q.k.intern(), q.v.intern()))
      case q: Query.In =>
        interner.computeIfAbsent(q, _ => Query.In(q.k.intern(), q.vs.map(_.intern())))
      case q: Query.HasKey =>
        interner.computeIfAbsent(q, _ => Query.HasKey(q.k.intern()))
      case q: Query.And =>
        interner.computeIfAbsent(q, _ => Query.And(intern(q.q1), intern(q.q2)))
      case q: Query.Or =>
        interner.computeIfAbsent(q, _ => Query.Or(intern(q.q1), intern(q.q2)))
      case q: Query.Not =>
        interner.computeIfAbsent(q, _ => Query.Not(intern(q.q)))
    }
  }

  private def parse(expression: String): Try[List[DataExprMeta]] = Try {
    val context = interpreter.execute(expression)
    val dataExprs = context.stack.flatMap {
      case ModelExtractors.PresentationType(t) => t.expr.dataExprs
      case _                                   => throw new IllegalArgumentException("expression is invalid")
    }

    // Offsets are not supported
    dataExprs.foreach { dataExpr =>
      if (!dataExpr.offset.isZero) {
        throw new IllegalArgumentException(
          s":offset not supported for streaming evaluation [[$dataExpr]]"
        )
      }
    }

    dataExprs.distinct.map { e =>
      val q = intern(compress(e.query))
      DataExprMeta(e, e.toString, q)
    }
  }

  def split(expression: String, frequency: Long): List[Subscription] = {
    exprCache.get(expression) match {
      case Success(dataExprs: List[DataExpr]) => dataExprs.map(e => toSubscription(e, frequency))
      case Failure(t)                         => throw t
    }
  }

  private def toSubscription(meta: DataExprMeta, frequency: Long): Subscription = {
    Subscription(meta.compressedQuery, ExpressionMetadata(meta.exprString, frequency))
  }

  private def simplify(query: Query): Query = {
    val newQuery = query match {
      case Query.And(Query.True, q)  => simplify(q)
      case Query.And(q, Query.True)  => simplify(q)
      case Query.Or(Query.True, _)   => Query.True
      case Query.Or(_, Query.True)   => Query.True
      case Query.And(Query.False, _) => Query.False
      case Query.And(_, Query.False) => Query.False
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

  private[lwcapi] def compress(expr: Query): Query = {
    val tmp = expr.rewrite { case kq: KeyQuery if !keepKeys.contains(kq.k) => Query.True }
    simplify(tmp.asInstanceOf[Query])
  }
}

object ExpressionSplitter {
  private case class DataExprMeta(expr: DataExpr, exprString: String, compressedQuery: Query)
}
