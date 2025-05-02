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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.util.BoundedPriorityBuffer
import com.netflix.atlas.core.util.Math

import java.util.Comparator

trait FilterExpr extends TimeSeriesExpr

object FilterExpr {

  /**
    * Represents a basic summary stat for an input time series.
    *
    * @param expr
    *     Input expression to compute the stat over.
    * @param stat
    *     Which summary stat to compute.
    * @param str
    *     Optional string representation of the expression. Used for common-cases of helper
    *     functions to avoid duplication of the original expression.
    */
  case class Stat(expr: TimeSeriesExpr, stat: String, str: Option[String] = None)
      extends FilterExpr {

    override def append(builder: java.lang.StringBuilder): Unit = {
      str match {
        case Some(s) => builder.append(s)
        case None    => Interpreter.append(builder, expr, stat, Interpreter.WordToken(":stat"))
      }
    }

    def dataExprs: List[DataExpr] = expr.dataExprs

    def isGrouped: Boolean = expr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

    def finalGrouping: List[String] = expr.finalGrouping

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val rs = expr.eval(context, data)
      val newData = rs.data.map { t =>
        val v = SummaryStats(t, context.start, context.end).get(stat)
        val seq = new FunctionTimeSeq(DsType.Gauge, context.step, _ => v)
        TimeSeries(t.tags, s"stat-$stat(${t.label})", seq)
      }
      ResultSet(this, newData, rs.state)
    }
  }

  trait StatExpr extends FilterExpr {

    def name: String

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, Interpreter.WordToken(s":stat-$name"))
    }

    def dataExprs: List[DataExpr] = Nil

    def isGrouped: Boolean = false

    def groupByKey(tags: Map[String, String]): Option[String] = None

    def finalGrouping: List[String] = Nil

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      ResultSet(this, Nil, context.state)
    }
  }

  case object StatAvg extends StatExpr {

    def name: String = "avg"
  }

  case object StatMax extends StatExpr {

    def name: String = "max"
  }

  case object StatMin extends StatExpr {

    def name: String = "min"
  }

  case object StatLast extends StatExpr {

    def name: String = "last"
  }

  case object StatCount extends StatExpr {

    def name: String = "count"
  }

  case object StatTotal extends StatExpr {

    def name: String = "total"
  }

  case class Filter(expr1: TimeSeriesExpr, expr2: TimeSeriesExpr) extends FilterExpr {

    if (!expr1.isGrouped) require(!expr2.isGrouped, "filter grouping must match expr grouping")

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr1, expr2, Interpreter.WordToken(":filter"))
    }

    def dataExprs: List[DataExpr] = expr1.dataExprs ::: expr2.dataExprs

    def isGrouped: Boolean = expr1.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr1.groupByKey(tags)

    def finalGrouping: List[String] = expr1.finalGrouping

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
          require(rs2.data.lengthCompare(1) == 0, "empty result for filter expression")
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

  /**
    * Base type for Top/Bottom K operators.
    */
  trait PriorityFilterExpr extends FilterExpr {

    /** Operation name to use for encoding expression. */
    def opName: String

    /** Comparator that determines the priority order. */
    def comparator: Comparator[TimeSeriesSummary]

    /** Aggregation to use for time series that are not one of the K highest priority. */
    def othersAggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      TimeSeries.NoopAggregator
    }

    /** Grouped expression to select the input from. */
    def expr: TimeSeriesExpr

    /** Summary statistic that should be used for the priority. */
    def stat: String

    /** Max number of entries to return. */
    def k: Int

    require(k > 0, s"k must be positive ($k <= 0)")

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, expr, stat, k, Interpreter.WordToken(s":$opName"))
    }

    def dataExprs: List[DataExpr] = expr.dataExprs

    def isGrouped: Boolean = expr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

    def finalGrouping: List[String] = expr.finalGrouping

    def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
      val buffer = new BoundedPriorityBuffer[TimeSeriesSummary](k, comparator)
      val aggregator = othersAggregator(context.start, context.end)
      val rs = expr.eval(context, data)
      rs.data.foreach { t =>
        val v = SummaryStats(t, context.start, context.end).get(stat)
        val other = buffer.add(TimeSeriesSummary(t, v))
        if (other != null) {
          aggregator.update(other.timeSeries)
        }
      }
      val newData = aggregator match {
        case aggr if aggr.isEmpty =>
          buffer.toList.map(_.timeSeries)
        case _ =>
          val others = aggregator.result()
          val otherTags = others.tags ++ finalGrouping.map(k => k -> "--others--").toMap
          val otherLabel = TimeSeries.toLabel(otherTags)
          others.withTags(otherTags).withLabel(otherLabel) :: buffer.toList.map(_.timeSeries)
      }
      ResultSet(this, newData, rs.state)
    }
  }

  case class TopK(expr: TimeSeriesExpr, stat: String, k: Int) extends PriorityFilterExpr {

    override def opName: String = "topk"

    override def comparator: Comparator[TimeSeriesSummary] = StatComparator.reversed()
  }

  case class TopKOthersMin(expr: TimeSeriesExpr, stat: String, k: Int) extends PriorityFilterExpr {

    override def opName: String = "topk-others-min"

    override def comparator: Comparator[TimeSeriesSummary] = StatComparator.reversed()

    override def othersAggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      new TimeSeries.SimpleAggregator(start, end, Math.minNaN)
    }
  }

  case class TopKOthersMax(expr: TimeSeriesExpr, stat: String, k: Int) extends PriorityFilterExpr {

    override def opName: String = "topk-others-max"

    override def comparator: Comparator[TimeSeriesSummary] = StatComparator.reversed()

    override def othersAggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      new TimeSeries.SimpleAggregator(start, end, Math.maxNaN)
    }
  }

  case class TopKOthersSum(expr: TimeSeriesExpr, stat: String, k: Int) extends PriorityFilterExpr {

    override def opName: String = "topk-others-sum"

    override def comparator: Comparator[TimeSeriesSummary] = StatComparator.reversed()

    override def othersAggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      new TimeSeries.SimpleAggregator(start, end, Math.addNaN)
    }
  }

  case class TopKOthersAvg(expr: TimeSeriesExpr, stat: String, k: Int) extends PriorityFilterExpr {

    override def opName: String = "topk-others-avg"

    override def comparator: Comparator[TimeSeriesSummary] = StatComparator.reversed()

    override def othersAggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      new TimeSeries.AvgAggregator(start, end)
    }
  }

  case class BottomK(expr: TimeSeriesExpr, stat: String, k: Int) extends PriorityFilterExpr {

    override def opName: String = "bottomk"

    override def comparator: Comparator[TimeSeriesSummary] = StatComparator
  }

  case class BottomKOthersMin(expr: TimeSeriesExpr, stat: String, k: Int)
      extends PriorityFilterExpr {

    override def opName: String = "bottomk-others-min"

    override def comparator: Comparator[TimeSeriesSummary] = StatComparator

    override def othersAggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      new TimeSeries.SimpleAggregator(start, end, Math.minNaN)
    }
  }

  case class BottomKOthersMax(expr: TimeSeriesExpr, stat: String, k: Int)
      extends PriorityFilterExpr {

    override def opName: String = "bottomk-others-max"

    override def comparator: Comparator[TimeSeriesSummary] = StatComparator

    override def othersAggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      new TimeSeries.SimpleAggregator(start, end, Math.maxNaN)
    }
  }

  case class BottomKOthersSum(expr: TimeSeriesExpr, stat: String, k: Int)
      extends PriorityFilterExpr {

    override def opName: String = "bottomk-others-sum"

    override def comparator: Comparator[TimeSeriesSummary] = StatComparator

    override def othersAggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      new TimeSeries.SimpleAggregator(start, end, Math.addNaN)
    }
  }

  case class BottomKOthersAvg(expr: TimeSeriesExpr, stat: String, k: Int)
      extends PriorityFilterExpr {

    override def opName: String = "bottomk-others-avg"

    override def comparator: Comparator[TimeSeriesSummary] = StatComparator

    override def othersAggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      new TimeSeries.AvgAggregator(start, end)
    }
  }

  /**
    * Caches the statistic value associated with a time series. Used for priority filters to
    * avoid recomputing the summary statistics on each comparison operation.
    */
  case class TimeSeriesSummary(timeSeries: TimeSeries, stat: Double)

  /** Default natural order comparator used for priority filters. */
  private object StatComparator extends Comparator[TimeSeriesSummary] {

    override def compare(t1: TimeSeriesSummary, t2: TimeSeriesSummary): Int = {
      java.lang.Double.compare(t1.stat, t2.stat)
    }
  }
}
