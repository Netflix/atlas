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

import java.time.Duration

import com.netflix.atlas.core.model.ConsolidationFunction.SumOrAvgCf
import com.netflix.atlas.core.util.Math
import com.netflix.atlas.core.util.SmallHashMap
import com.netflix.atlas.core.util.Strings

sealed trait DataExpr extends TimeSeriesExpr with Product {

  def query: Query

  def cf: ConsolidationFunction

  def offset: Duration

  def dataExprs: List[DataExpr] = List(this)

  def isGrouped: Boolean = false

  def groupByKey(tags: Map[String, String]): Option[String] = None

  def finalGrouping: List[String] = Nil

  def exprString: String

  override def eval(context: EvalContext, data: List[TimeSeries]): ResultSet = {
    val filtered = data.filter(t => query.matches(t.tags))
    val evaluator = incrementalEvaluator(context)
    val rs = filtered.flatMap(t => evaluator.update(this, t)) ::: evaluator.flush()
    ResultSet(this, rs, context.state)
  }

  protected def consolidate(step: Long, ts: List[TimeSeries]): List[TimeSeries] = {
    ts.map { t =>
      val offsetStr = Strings.toString(offset)
      val label = if (offset.isZero) t.label else s"${t.label} (offset=$offsetStr)"
      if (step == t.data.step) t.withLabel(label)
      else {
        TimeSeries(t.tags, label, new MapStepTimeSeq(t.data, step, cf))
      }
    }
  }

  protected def commonTags(tags: Map[String, String]): Map[String, String] = {
    val keys = Query.exactKeys(query)
    val result = tags.filter(t => keys.contains(t._1))
    if (result.isEmpty) DataExpr.unknown else result
  }

  override def toString: String = {
    if (offset.isZero) exprString else s"$exprString,$offset,:offset"
  }

  /**
    * Hash code is cached to allow cheaper lookup during evaluation. This implementation
    * in the base interface depends on the main fields of the case class being set prior
    * to `super()` being called in the case class constructor. That appears to be the case
    * with current scala versions.
    */
  override val hashCode: Int = scala.util.hashing.MurmurHash3.productHash(this)
}

object DataExpr {

  private val unknown = SmallHashMap("name" -> "unknown")

  private def defaultLabel(expr: DataExpr, ts: TimeSeries): String = {
    val label = expr match {
      case af: AggregateFunction => af.labelString
      case by: GroupBy           => by.keyString(ts.tags)
      case _                     => TimeSeries.toLabel(ts.tags)
    }
    val offset = expr.offset
    if (offset.isZero) label else s"$label (offset=${Strings.toString(offset)})"
  }

  def withDefaultLabel(expr: DataExpr, ts: TimeSeries): TimeSeries = {
    ts.withLabel(defaultLabel(expr, ts))
  }

  /**
    * Create a group by key string from the set of keys and a tag list. If a key from the input
    * is missing from the tags, then it will return null.
    */
  def keyString(keys: List[String], tags: Map[String, String]): String = {

    // 32 is typically big enough to prevent a resize with a single key
    val builder = new StringBuilder(32 * keys.size)
    builder.append('(')
    val it = keys.iterator
    while (it.hasNext) {
      val k = it.next()
      val v = tags.get(k)
      if (v.isEmpty) return null
      builder.append(k).append('=').append(v.get).append(' ')
    }
    builder(builder.length - 1) = ')'
    builder.toString
  }

  def allKeys(dataExpr: DataExpr): Set[String] = {
    Query.allKeys(dataExpr.query) ++ dataExpr.finalGrouping
  }

  case class All(query: Query, offset: Duration = Duration.ZERO) extends DataExpr {

    def cf: ConsolidationFunction = ConsolidationFunction.Sum

    override def withOffset(d: Duration): All = copy(offset = d)

    override def exprString: String = s"$query,:all"

    override def incrementalEvaluator(context: EvalContext): IncrementalEvaluator = {
      val self = this
      new IncrementalEvaluator {
        override def orderBy: List[String] = Nil

        override def update(dataExpr: DataExpr, ts: TimeSeries): List[TimeSeries] = {
          if (dataExpr == self) List(ts) else Nil
        }

        override def flush(): List[TimeSeries] = Nil

        override def state: Map[StatefulExpr, Any] = context.state
      }
    }
  }

  sealed trait AggregateFunction extends DataExpr {

    def labelString: String

    def withConsolidation(f: ConsolidationFunction): AggregateFunction

    def aggregator(start: Long, end: Long): TimeSeries.Aggregator

    override def incrementalEvaluator(context: EvalContext): IncrementalEvaluator = {
      val self = this
      new IncrementalEvaluator {
        private var aggr: TimeSeries.Aggregator = _

        override def orderBy: List[String] = Nil

        override def update(dataExpr: DataExpr, ts: TimeSeries): List[TimeSeries] = {
          if (dataExpr == self) {
            if (aggr == null)
              aggr = aggregator(context.start, context.end)
            aggr.update(ts)
          }
          Nil
        }

        override def flush(): List[TimeSeries] = {
          if (aggr == null) {
            List(TimeSeries.noData(query, context.step))
          } else {
            val ts = aggr.result()
            val tags = commonTags(ts.tags)
            val tmp = TimeSeries(tags, TimeSeries.toLabel(tags), ts.data)
            consolidate(context.step, List(tmp))
          }
        }

        override def state: Map[StatefulExpr, Any] = context.state
      }
    }
  }

  case class Sum(
    query: Query,
    cf: SumOrAvgCf = ConsolidationFunction.Avg,
    offset: Duration = Duration.ZERO
  ) extends AggregateFunction {

    override def withConsolidation(f: ConsolidationFunction): AggregateFunction = f match {
      case v: SumOrAvgCf => copy(cf = v)
      case v             => Consolidation(copy(query = query, offset = offset), v)
    }

    override def withOffset(d: Duration): Sum = copy(offset = d)

    override def labelString: String = s"sum(${query.labelString})"

    override def exprString: String = {
      if (cf == ConsolidationFunction.Avg) s"$query,:sum" else s"$query,:sum,$cf"
    }

    override def aggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      new TimeSeries.SimpleAggregator(start, end, Math.addNaN)
    }
  }

  case class Count(
    query: Query,
    cf: SumOrAvgCf = ConsolidationFunction.Avg,
    offset: Duration = Duration.ZERO
  ) extends AggregateFunction {

    override def withConsolidation(f: ConsolidationFunction): AggregateFunction = f match {
      case v: SumOrAvgCf => copy(cf = v)
      case v             => Consolidation(copy(query = query, offset = offset), v)
    }

    override def withOffset(d: Duration): Count = copy(offset = d)

    override def labelString: String = s"count(${query.labelString})"

    override def exprString: String = {
      if (cf == ConsolidationFunction.Avg) s"$query,:count" else s"$query,:count,$cf"
    }

    override def aggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      new TimeSeries.CountAggregator(start, end)
    }
  }

  case class Min(query: Query, offset: Duration = Duration.ZERO) extends AggregateFunction {

    def cf: ConsolidationFunction = ConsolidationFunction.Min

    override def withConsolidation(f: ConsolidationFunction): AggregateFunction = {
      if (f == ConsolidationFunction.Min) this else Consolidation(this, f)
    }

    override def withOffset(d: Duration): Min = copy(offset = d)

    override def labelString: String = s"min(${query.labelString})"

    override def exprString: String = s"$query,:min"

    override def aggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      new TimeSeries.SimpleAggregator(start, end, Math.minNaN)
    }
  }

  case class Max(query: Query, offset: Duration = Duration.ZERO) extends AggregateFunction {

    def cf: ConsolidationFunction = ConsolidationFunction.Max

    override def withConsolidation(f: ConsolidationFunction): AggregateFunction = {
      if (f == ConsolidationFunction.Max) this else Consolidation(this, f)
    }

    override def withOffset(d: Duration): Max = copy(offset = d)

    override def labelString: String = s"max(${query.labelString})"

    override def exprString: String = s"$query,:max"

    override def aggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      new TimeSeries.SimpleAggregator(start, end, Math.maxNaN)
    }
  }

  case class Consolidation(af: AggregateFunction, cf: ConsolidationFunction)
      extends AggregateFunction {

    override def eval(context: EvalContext, data: List[TimeSeries]): ResultSet = {
      af.eval(context, data)
    }

    def query: Query = af.query

    def offset: Duration = af.offset

    override def withConsolidation(f: ConsolidationFunction): AggregateFunction = {
      af.withConsolidation(f)
    }

    override def withOffset(d: Duration): Consolidation = {
      Consolidation(af.withOffset(d).asInstanceOf[AggregateFunction], cf)
    }

    override def labelString: String = af.labelString

    override def exprString: String = s"$af,$cf"

    override def aggregator(start: Long, end: Long): TimeSeries.Aggregator = {
      af.aggregator(start, end)
    }
  }

  case class GroupBy(af: AggregateFunction, keys: List[String]) extends DataExpr {

    def query: Query = af.query

    def cf: ConsolidationFunction = af.cf

    def offset: Duration = af.offset

    override def withOffset(d: Duration): GroupBy = {
      copy(af = af.withOffset(d).asInstanceOf[AggregateFunction])
    }

    override def isGrouped: Boolean = true

    def keyString(tags: Map[String, String]): String = DataExpr.keyString(keys, tags)

    override def groupByKey(tags: Map[String, String]): Option[String] = Option(keyString(tags))

    override def finalGrouping: List[String] = keys

    override def exprString: String = s"$af,(,${keys.mkString(",")},),:by"

    override def eval(context: EvalContext, data: List[TimeSeries]): ResultSet = {
      val filtered = data
        .filter(t => query.matches(t.tags))
        .groupBy(t => keyString(t.tags))
        .filter(_._1 != null)
        .toList
        .sortWith(_._1 < _._1)
        .flatMap(_._2)
      val evaluator = incrementalEvaluator(context)
      val rs = filtered.flatMap(t => evaluator.update(this, t)) ::: evaluator.flush()
      ResultSet(this, rs, context.state)
    }

    override def incrementalEvaluator(context: EvalContext): IncrementalEvaluator = {
      val self = this
      new IncrementalEvaluator {
        private val ks = Query.exactKeys(query) ++ keys
        private var currentKey: String = _
        private var aggr: TimeSeries.Aggregator = _

        override def orderBy: List[String] = keys

        override def update(dataExpr: DataExpr, ts: TimeSeries): List[TimeSeries] = {
          if (dataExpr != self) return Nil
          var result = List.empty[TimeSeries]
          val groupingKey = keyString(ts.tags)
          if (groupingKey == null) {
            result
          } else {
            if (currentKey != groupingKey) {
              result = flush()
              currentKey = groupingKey
              aggr = af.aggregator(context.start, context.end)
            }
            aggr.update(ts)
            result
          }
        }

        override def flush(): List[TimeSeries] = {
          if (currentKey != null) {
            val tmp = aggr.result()
            val tags = tmp.tags.filter(t => ks.contains(t._1))
            consolidate(context.step, List(TimeSeries(tags, currentKey, tmp.data)))
          } else {
            Nil
          }
        }

        override def state: Map[StatefulExpr, Any] = context.state
      }
    }
  }
}
