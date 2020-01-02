/*
 * Copyright 2014-2020 Netflix, Inc.
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

import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.Caffeine
import com.netflix.atlas.core.model.CustomVocabulary
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.Query.KeyQuery
import com.netflix.atlas.core.stacklang.Interpreter
import com.typesafe.config.Config

import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Splits a complete graph expression (StyleExpr) string into a set of subscriptions. Each
  * subscription is based on the underlying data expressions (DataExpr) that get pushed back
  * to the systems supplying data to LWCAPI.
  */
class ExpressionSplitter(config: Config) {

  import ExpressionSplitter._

  private val keepKeys = Set("nf.app", "nf.stack", "nf.cluster")

  private val interpreter = Interpreter(new CustomVocabulary(config).allWords)

  /**
    * Processing the expressions can be quite expensive. In particular compiling regular
    * expressions to ensure they are valid. Generally the set of expressions should not
    * vary much over time and the evaluator library will regularly submit the full list
    * to sync with. This cache prevents the reprocessing for expressions that have already
    * been seen recently.
    *
    * Note: do not use LoadingCache, see `getFromCache`.
    */
  private val exprCache = Caffeine
    .newBuilder()
    .expireAfterAccess(10, TimeUnit.MINUTES)
    .build[String, Try[List[DataExprMeta]]]()

  /**
    * Cache used to reduce the memory overhead of the query objects.
    */
  private val interner = Caffeine
    .newBuilder()
    .expireAfterAccess(12, TimeUnit.HOURS)
    .build[Query, Query]()

  /**
    * On instance types with a lot of cores, the loading cache causes a lot of thread
    * contention and most threads are blocked. This just does and get/put which potentially
    * recomputes some values, but for this case that is preferable.
    */
  private def internQuery(q: Query, newQuery: => Query): Query = {
    val cached = interner.getIfPresent(q)
    if (cached == null) {
      val tmp = newQuery
      interner.put(tmp, tmp)
      tmp
    } else {
      cached
    }
  }

  private[lwcapi] def intern(query: Query): Query = {
    query match {
      case Query.True =>
        query
      case Query.False =>
        query
      case q: Query.Equal =>
        internQuery(q, Query.Equal(q.k.intern(), q.v.intern()))
      case q: Query.LessThan =>
        internQuery(q, Query.LessThan(q.k.intern(), q.v.intern()))
      case q: Query.LessThanEqual =>
        internQuery(q, Query.LessThanEqual(q.k.intern(), q.v.intern()))
      case q: Query.GreaterThan =>
        internQuery(q, Query.GreaterThan(q.k.intern(), q.v.intern()))
      case q: Query.GreaterThanEqual =>
        internQuery(q, Query.GreaterThanEqual(q.k.intern(), q.v.intern()))
      case q: Query.Regex =>
        internQuery(q, Query.Regex(q.k.intern(), q.v.intern()))
      case q: Query.RegexIgnoreCase =>
        internQuery(q, Query.RegexIgnoreCase(q.k.intern(), q.v.intern()))
      case q: Query.In =>
        internQuery(q, Query.In(q.k.intern(), q.vs.map(_.intern())))
      case q: Query.HasKey =>
        internQuery(q, Query.HasKey(q.k.intern()))
      case q: Query.And =>
        internQuery(q, Query.And(intern(q.q1), intern(q.q2)))
      case q: Query.Or =>
        internQuery(q, Query.Or(intern(q.q1), intern(q.q2)))
      case q: Query.Not =>
        internQuery(q, Query.Not(intern(q.q)))
    }
  }

  private def parse(expression: String): Try[List[DataExprMeta]] = Try {
    val context = interpreter.execute(expression)
    val dataExprs = context.stack.flatMap {
      case ModelExtractors.PresentationType(t) => t.perOffset.flatMap(_.expr.dataExprs)
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

  /**
    * On instance types with a lot of cores, the loading cache causes a lot of thread
    * contention and most threads are blocked. This just does and get/put which potentially
    * recomputes some values, but for this case that is preferable.
    */
  private def getFromCache(k: String): Try[List[DataExprMeta]] = {
    val value = exprCache.getIfPresent(k)
    if (value == null) {
      val tmp = parse(k)
      exprCache.put(k, tmp)
      tmp
    } else {
      value
    }
  }

  def split(expression: String, frequency: Long): List[Subscription] = {
    getFromCache(expression) match {
      case Success(exprs: List[_]) => exprs.map(e => toSubscription(e, frequency))
      case Failure(t)              => throw t
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
