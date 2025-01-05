/*
 * Copyright 2014-2025 Netflix, Inc.
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
import com.netflix.atlas.core.model.EventExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.Query.KeyQuery
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.TraceQuery
import com.netflix.atlas.eval.model.ExprType
import com.netflix.atlas.eval.stream.ExprInterpreter
import com.netflix.spectator.atlas.impl.Parser
import com.netflix.spectator.ipc.ServerGroup
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

  import ExpressionSplitter.*

  private val keepKeys = Set("nf.app", "nf.cluster", "nf.shard1", "nf.shard2", "nf.stack")

  private val interpreter = new ExprInterpreter(config)

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

  private def toSpectatorQuery(query: Query): SpectatorQuery = {
    Parser.parseQuery(query.toString)
  }

  private def parse(expression: String, exprType: ExprType): Try[List[DataExprMeta]] = Try {
    val parsedExpressions = interpreter.parseQuery(expression, exprType)
    exprType match {
      case ExprType.EVENTS =>
        parsedExpressions.collect {
          case e: EventExpr =>
            val q = toSpectatorQuery(compress(e.query))
            DataExprMeta(e.toString, q)
        }
      case ExprType.TIME_SERIES =>
        parsedExpressions
          .collect {
            case se: StyleExpr => se.expr.dataExprs
          }
          .flatten
          .distinct
          .map { e =>
            val q = toSpectatorQuery(compress(e.query))
            DataExprMeta(e.toString, q)
          }
      case ExprType.TRACE_EVENTS =>
        parsedExpressions.map { e =>
          // Tracing cannot be scoped to specific infrastructure, always use True
          DataExprMeta(e.toString, MatchesAll)
        }
      case ExprType.TRACE_TIME_SERIES =>
        parsedExpressions
          .collect {
            case tq: TraceQuery.SpanTimeSeries =>
              tq.expr.expr.dataExprs.map(e => tq.copy(expr = StyleExpr(e, Map.empty)))
          }
          .flatten
          .distinct
          .map { e =>
            // Tracing cannot be scoped to specific infrastructure, always use True
            DataExprMeta(e.toString, MatchesAll)
          }
    }
  }

  /**
    * On instance types with a lot of cores, the loading cache causes a lot of thread
    * contention and most threads are blocked. This just does and get/put which potentially
    * recomputes some values, but for this case that is preferable.
    */
  private def getFromCache(k: String, exprType: ExprType): Try[List[DataExprMeta]] = {
    val key = s"$k:$exprType"
    val value = exprCache.getIfPresent(key)
    if (value == null) {
      val tmp = parse(k, exprType)
      exprCache.put(key, tmp)
      tmp
    } else {
      value
    }
  }

  def split(expression: String, exprType: ExprType, frequency: Long): List[Subscription] = {
    getFromCache(expression, exprType) match {
      case Success(exprs: List[?]) => exprs.map(e => toSubscription(e, exprType, frequency))
      case Failure(t)              => throw t
    }
  }

  private def toSubscription(
    meta: DataExprMeta,
    exprType: ExprType,
    frequency: Long
  ): Subscription = {
    Subscription(meta.compressedQuery, ExpressionMetadata(meta.exprString, exprType, frequency))
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
    val query = expr
      .rewrite {
        // Rewrite exact match for nf.asg to an exact match for the nf.cluster. When
        // fetching expressions for a cluster, this will allow expressions with only
        // a check for the asg to get filtered appropriately.
        case q @ Query.Equal("nf.asg", asg) =>
          val cluster = ServerGroup.parse(asg).cluster()
          if (cluster == null) q else Query.Equal("nf.cluster", cluster)
      }
      .rewrite {
        case kq: KeyQuery if !keepKeys.contains(kq.k) => Query.True
      }
      .asInstanceOf[Query]
    simplify(query)
  }
}

object ExpressionSplitter {
  private case class DataExprMeta(exprString: String, compressedQuery: SpectatorQuery)
}
