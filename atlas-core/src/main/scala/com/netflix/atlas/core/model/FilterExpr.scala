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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.util.Math

trait FilterExpr extends TimeSeriesExpr

object FilterExpr {

  case class Stat(expr: TimeSeriesExpr, stat: String) extends FilterExpr {

    override def toString: String = s"$expr,$stat,:stat"

    def dataExprs: List[DataExpr] = expr.dataExprs

    def isGrouped: Boolean = expr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

    private def value(s: SummaryStats): Double = stat match {
      case "max"   => s.max
      case "min"   => s.min
      case "total" => s.total
      case "avg"   => s.avg
    }

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      val newData = rs.data.map { t =>
        val v = value(SummaryStats(t, context.start, context.end))
        val seq = new FunctionTimeSeq(DsType.Gauge, context.step, _ => v)
        TimeSeries(t.tags, s"stat-$stat(${t.label})", seq)
      }
      ResultSet(this, newData, rs.state)
    }
  }

  trait StatExpr extends FilterExpr {
    def name: String

    override def toString: String = s":stat-$name"

    def dataExprs: List[DataExpr] = Nil

    def isGrouped: Boolean = false

    def groupByKey(tags: Map[String, String]): Option[String] = None

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      ResultSet(this, Nil, context.state)
    }
  }

  case object StatMax extends StatExpr {
    def name: String = "max"
  }

  case object StatMin extends StatExpr {
    def name: String = "min"
  }

  case object StatAvg extends StatExpr {
    def name: String = "avg"
  }

  case object StatTotal extends StatExpr {
    def name: String = "total"
  }

  case class Filter(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends FilterExpr {

    if (!expr1.isGrouped) require(!expr2.isGrouped, "filter grouping must match expr grouping")

    override def toString: String = s"$expr1,$expr2,:filter"

    def dataExprs: List[DataExpr] = expr1.dataExprs ::: expr2.dataExprs

    def isGrouped: Boolean = expr1.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr1.groupByKey(tags)

    def matches(context: EvalContext, ts: TimeSeries): Boolean = {
      var m = false
      ts.data.foreach(context.start, context.end) { (_, v) =>
        m = m || Math.toBoolean(v)
      }
      m
    }

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs1 = expr1.eval(context, data)
      val rs2 = expr2.eval(context, data)
      val result = (expr1.isGrouped, expr2.isGrouped) match {
        case (_, false) =>
          require(rs2.data.size == 1)
          if (matches(context, rs2.data.head)) rs1.data else Nil
        case (false, _) =>
          // Shouldn't be able to get here
          Nil
        case (true, true) =>
          val g2 = rs2.data.groupBy(t => groupByKey(t.tags))
          rs1.data.filter { t1 =>
            val k = groupByKey(t1.tags)
            g2.get(k).fold(false) {
              case t2 :: Nil => matches(context, t2)
              case _         => false
            }
          }
      }
      val msg = s"${result.size} of ${rs1.data.size} lines matched filter"
      ResultSet(this, result, rs1.state ++ rs2.state, List(msg))
    }
  }
}
