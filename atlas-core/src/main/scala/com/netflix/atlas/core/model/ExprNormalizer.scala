/*
 * Copyright 2014-2026 Netflix, Inc.
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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.util.RefIntHashMap
import com.netflix.atlas.core.util.Strings
import com.typesafe.config.Config

class ExprNormalizer(config: Config) {

  private val queryOrdering: Ordering[Query] = {
    import scala.jdk.CollectionConverters.*
    val prefixKeys = config.getStringList("prefix-keys").asScala.toList
    val suffixKeys = config.getStringList("suffix-keys").asScala.toList
    ExprNormalizer.newQueryOrdering(prefixKeys, suffixKeys)
  }

  def normalize(expr: StyleExpr): StyleExpr = {
    val legendNormalized = ExprNormalizer.normalizeLegendVars(expr)
    val statNormalized = ExprNormalizer.normalizeStat(legendNormalized)
    statNormalized
      .rewrite {
        case q: Query => sort(q)
      }
      .asInstanceOf[StyleExpr]
  }

  def normalizeToString(expr: StyleExpr): String = {
    normalize(expr).toString
      .replace(",:const", "")
      .replace(",:line", "")
  }

  private def sort(query: Query): Query = {
    val simplified = Query.simplify(query)
    val normalized = Query
      .dnfList(simplified)
      .map { q =>
        Query
          .cnfList(q)
          .map(ExprNormalizer.normalizeClauses)
          .distinct
          .sorted(queryOrdering)
      }
      .distinct
    ExprNormalizer
      .removeRedundantClauses(normalized)
      .map { qs =>
        qs.reduce { (q1, q2) =>
          Query.And(q1, q2)
        }
      }
      .sortWith(_.toString < _.toString)
      .reduce { (q1, q2) =>
        Query.Or(q1, q2)
      }
  }
}

object ExprNormalizer {

  private[model] def normalizeLegendVars(expr: StyleExpr): StyleExpr = {
    expr.settings.get("legend").fold(expr) { legend =>
      val settings = expr.settings + ("legend" -> Strings.substitute(legend, k => s"$$($k)"))
      expr.copy(settings = settings)
    }
  }

  private[model] def normalizeStat(expr: StyleExpr): StyleExpr = {
    expr
      .rewrite {
        case FilterExpr.Filter(ts1, ts2) =>
          val updated = ts2.rewrite {
            case FilterExpr.Stat(ts, s, None) if ts == ts1 =>
              s match {
                case "avg"   => FilterExpr.StatAvg
                case "min"   => FilterExpr.StatMin
                case "max"   => FilterExpr.StatMax
                case "last"  => FilterExpr.StatLast
                case "total" => FilterExpr.StatTotal
                case "count" => FilterExpr.StatCount
                case _       => FilterExpr.Stat(ts, s, None)
              }
          }
          FilterExpr.Filter(ts1, updated.asInstanceOf[TimeSeriesExpr])
      }
      .asInstanceOf[StyleExpr]
  }

  private[model] def normalizeClauses(query: Query): Query = query match {
    case Query.In(k, vs) =>
      val values = vs.sorted.distinct
      if (values.lengthCompare(1) == 0)
        Query.Equal(k, values.head)
      else
        Query.In(k, values)
    case q => q
  }

  private[model] def removeRedundantClauses(
    queries: List[List[Query]]
  ): List[List[Query]] = {
    queries match {
      case Nil      => queries
      case _ :: Nil => queries
      case _ =>
        val indexed = queries.map(q => q -> q.toSet)
        indexed
          .filterNot {
            case (_, qSet) =>
              indexed.forall { case (_, s) => s.subsetOf(qSet) }
          }
          .map(_._1)
    }
  }

  private def newQueryOrdering(
    prefixKeys: List[String],
    suffixKeys: List[String]
  ): Ordering[Query] = {
    val prefixPositionMap = createPositionMap(prefixKeys)
    val suffixPositionMap = createPositionMap(suffixKeys)

    (q1: Query, q2: Query) => {
      val k1 = key(q1)
      val k2 = key(q2)

      if (k1 == k2) {
        q1.toString.compare(q2.toString)
      } else {
        val p1 = prefixPositionMap.get(k1, -1)
        val p2 = prefixPositionMap.get(k2, -1)
        val s1 = suffixPositionMap.get(k1, -1)
        val s2 = suffixPositionMap.get(k2, -1)

        if (p1 >= 0 && p2 >= 0) {
          p1.compare(p2)
        } else if (p1 >= 0) {
          -1
        } else if (p2 >= 0) {
          1
        } else if (s1 >= 0 && s2 < 0) {
          1
        } else if (s2 >= 0 && s1 < 0) {
          -1
        } else if (s1 >= 0 && s2 >= 0) {
          s1.compare(s2)
        } else {
          q1.toString.compare(q2.toString)
        }
      }
    }
  }

  private def createPositionMap(vs: List[String]): RefIntHashMap[String] = {
    val positions = new RefIntHashMap[String](vs.size)
    vs.zipWithIndex.foreach {
      case (v, i) => positions.put(v, i)
    }
    positions
  }

  @scala.annotation.tailrec
  private def key(query: Query): String = {
    query match {
      case q: Query.KeyQuery => q.k
      case Query.And(q1, _)  => key(q1)
      case Query.Or(q1, _)   => key(q1)
      case Query.Not(q)      => key(q)
      case _                 => ""
    }
  }
}
