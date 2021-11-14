/*
 * Copyright 2014-2021 Netflix, Inc.
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

import com.netflix.atlas.core.util.BoundedPriorityBuffer
import com.netflix.atlas.core.util.Math

import java.util.Comparator

trait FilterExpr extends TimeSeriesExpr

object FilterExpr {

  case class Stat(expr: TimeSeriesExpr, stat: String) extends FilterExpr {

    override def toString: String = s"$expr,$stat,:stat"

    def dataExprs: List[DataExpr] = expr.dataExprs

    def isGrouped: Boolean = expr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

    def finalGrouping: List[String] = expr.finalGrouping

    override def incrementalEvaluator(context: EvalContext): IncrementalEvaluator = {
      expr.incrementalEvaluator(context).map { t =>
        val v = SummaryStats(t, context.start, context.end).get(stat)
        val seq = new FunctionTimeSeq(DsType.Gauge, context.step, _ => v)
        TimeSeries(t.tags, s"stat-$stat(${t.label})", seq)
      }
    }
  }

  trait StatExpr extends FilterExpr {

    def name: String

    override def toString: String = s":stat-$name"

    def dataExprs: List[DataExpr] = Nil

    def isGrouped: Boolean = false

    def groupByKey(tags: Map[String, String]): Option[String] = None

    def finalGrouping: List[String] = Nil

    override def incrementalEvaluator(context: EvalContext): IncrementalEvaluator = {
      IncrementalEvaluator(Nil)
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

    override def toString: String = s"$expr1,$expr2,:filter"

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

    override def incrementalEvaluator(context: EvalContext): IncrementalEvaluator = {
      new IncrementalEvaluator {
        private val evaluator1 = expr1.incrementalEvaluator(context)
        private val evaluator2 = expr2.incrementalEvaluator(context)

        private var pending1 = List.empty[TimeSeries]
        private var pending2 = List.empty[TimeSeries]

        override def orderBy: List[String] = evaluator1.orderBy

        private def compareTo(o1: Option[String], o2: Option[String]): Int = {
          (o1, o2) match {
            case (None, None)         => 0
            case (None, Some(_))      => -1
            case (Some(_), None)      => 1
            case (Some(v1), Some(v2)) => v1.compareTo(v2)
          }
        }

        private def groupedMerge: List[TimeSeries] = {
          val groupByF = expr1.groupByKey _
          val builder = List.newBuilder[TimeSeries]
          while (pending1.nonEmpty && pending2.nonEmpty) {
            val k1 = groupByF(pending1.head.tags)
            val k2 = groupByF(pending2.head.tags)
            compareTo(k1, k2) match {
              case c if c < 0 =>
                pending1 = pending1.tail
              case c if c == 0 =>
                val t1 = pending1.head
                val t2 = pending2.head
                if (matches(context, t2))
                  builder += t1
                pending1 = pending1.tail
              case c if c > 0 =>
                pending2 = pending2.tail
            }
          }
          builder.result()
        }

        private def nonGroupedMerge: List[TimeSeries] = {
          val builder = List.newBuilder[TimeSeries]
          while (pending1.nonEmpty && pending2.nonEmpty) {
            val t1 = pending1.head
            val t2 = pending2.head
            if (matches(context, t2))
              builder += t1
            pending1 = pending1.tail
          }
          builder.result()
        }

        override def update(dataExpr: DataExpr, ts: TimeSeries): List[TimeSeries] = {
          val t1 = evaluator1.update(dataExpr, ts)
          pending1 = pending1 ::: t1
          val t2 = evaluator2.update(dataExpr, ts)
          pending2 = pending2 ::: t2
          if (expr2.isGrouped) groupedMerge else nonGroupedMerge
        }

        override def flush(): List[TimeSeries] = {
          pending1 = pending1 ::: evaluator1.flush()
          pending2 = pending2 ::: evaluator2.flush()
          if (expr2.isGrouped) groupedMerge else nonGroupedMerge
        }

        override def state: Map[StatefulExpr, Any] = {
          evaluator1.state ++ evaluator2.state
        }
      }
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

    override def toString: String = s"$expr,$stat,$k,:$opName"

    def dataExprs: List[DataExpr] = expr.dataExprs

    def isGrouped: Boolean = expr.isGrouped

    def groupByKey(tags: Map[String, String]): Option[String] = expr.groupByKey(tags)

    def finalGrouping: List[String] = expr.finalGrouping

    override def incrementalEvaluator(context: EvalContext): IncrementalEvaluator = {
      new IncrementalEvaluator {
        private val exprEvaluator = expr.incrementalEvaluator(context)
        private val buffer = new BoundedPriorityBuffer[TimeSeriesSummary](k, comparator)
        private val aggregator = othersAggregator(context.start, context.end)

        override def orderBy: List[String] = exprEvaluator.orderBy

        private def update(t: TimeSeries): Unit = {
          val v = SummaryStats(t, context.start, context.end).get(stat)
          val other = buffer.add(TimeSeriesSummary(t, v))
          if (other != null) {
            aggregator.update(other.timeSeries)
          }
        }

        override def update(dataExpr: DataExpr, ts: TimeSeries): List[TimeSeries] = {
          exprEvaluator.update(dataExpr, ts).foreach(update)
          Nil
        }

        override def flush(): List[TimeSeries] = {
          exprEvaluator.flush().foreach(update)
          aggregator match {
            case aggr if aggr.isEmpty =>
              buffer.toList.map(_.timeSeries)
            case _ =>
              val others = aggregator.result()
              val otherTags = others.tags ++ finalGrouping.map(k => k -> "--others--").toMap
              others.withTags(otherTags) :: buffer.toList.map(_.timeSeries)
          }
        }

        override def state: Map[StatefulExpr, Any] = exprEvaluator.state
      }
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
