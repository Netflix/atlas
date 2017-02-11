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

import java.time.Duration

import com.netflix.atlas.core.model.ConsolidationFunction.SumOrAvgCf
import com.netflix.atlas.core.util.Math
import com.netflix.atlas.core.util.SmallHashMap
import com.netflix.atlas.core.util.Strings

sealed trait DataExpr extends TimeSeriesExpr {
  def query: Query
  def cf: ConsolidationFunction
  def offset: Duration

  def dataExprs: List[DataExpr] = List(this)

  def isGrouped: Boolean = false

  def groupByKey(tags: Map[String, String]): Option[String] = {
    Some("%40X".format(TaggedItem.computeId(tags)))
  }

  def exprString: String

  protected def consolidate(step: Long, ts: List[TimeSeries]): List[TimeSeries] = {
    ts.map { t =>
      val offsetStr = Strings.toString(offset)
      val label = if (offset.isZero) t.label else s"${t.label} (offset=$offsetStr)"
      if (step == t.data.step) t.withLabel(label) else {
        TimeSeries(t.tags, label, new MapStepTimeSeq(t.data, step, cf))
      }
    }
  }

  protected def commonTags(tags: Map[String, String]): Map[String, String] = {
    val keys = Query.exactKeys(query)
    val result = tags.filter(t => keys.contains(t._1))
    if (result.isEmpty) DataExpr.unknown else result
  }

  def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
    ResultSet(this, data.getOrElse(this, Nil), context.state)
  }

  override def toString: String = {
    if (offset.isZero) exprString else s"$exprString,$offset,:offset"
  }
}

object DataExpr {

  private val unknown = SmallHashMap("name" -> "unknown")

  private def defaultLabel(expr: DataExpr, ts: TimeSeries): String = {
    val label = expr match {
      case af: AggregateFunction => af.labelString
      case by: GroupBy           => by.keyString(ts.tags)
      case Head(df, _)           => defaultLabel(df, ts)
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
    keys.foreach { k =>
      val v = tags.get(k)
      if (v.isEmpty) return null
      builder.append(k).append('=').append(v.get).append(' ')
    }
    builder(builder.length - 1) = ')'
    builder.toString
  }

  case class All(query: Query, offset: Duration = Duration.ZERO) extends DataExpr {
    def cf: ConsolidationFunction = ConsolidationFunction.Sum
    override def withOffset(d: Duration): All = copy(offset = d)
    override def exprString: String = s"$query,:all"

    override def eval(context: EvalContext, data: List[TimeSeries]): ResultSet = {
      val rs = consolidate(context.step, data.filter(t => query.matches(t.tags)))
      ResultSet(this, rs, context.state)
    }
  }

  sealed trait AggregateFunction extends DataExpr with BinaryOp {
    def labelString: String

    def withConsolidation(f: ConsolidationFunction): AggregateFunction

    override def eval(context: EvalContext, data: List[TimeSeries]): ResultSet = {
      val filtered = data.filter(t => query.matches(t.tags))
      val aggr = if (filtered.isEmpty) TimeSeries.noData(query, context.step) else {
        val tags = commonTags(filtered.head.tags)
        val t = TimeSeries.aggregate(filtered.iterator, context.start, context.end, this)
        TimeSeries(tags, TimeSeries.toLabel(tags), t.data)
      }
      val rs = consolidate(context.step, List(aggr))
      ResultSet(this, rs, context.state)
    }

    // Make sure we get toString locally rather than from BinaryOp
    override def toString: String = {
      if (offset.isZero) exprString else s"$exprString,$offset,:offset"
    }
  }

  case class Sum(
      query: Query,
      cf: SumOrAvgCf = ConsolidationFunction.Avg,
      offset: Duration = Duration.ZERO) extends AggregateFunction {

    override def withConsolidation(f: ConsolidationFunction): AggregateFunction = f match {
      case v: SumOrAvgCf => copy(cf = v)
      case v             => Consolidation(copy(query = query, offset= offset), v)
    }

    override def withOffset(d: Duration): Sum = copy(offset = d)

    def labelString: String = s"sum(${query.labelString})"

    override def exprString: String = {
      if (cf == ConsolidationFunction.Avg) s"$query,:sum" else s"$query,:sum,$cf"
    }

    def apply(v1: Double, v2: Double): Double = Math.addNaN(v1, v2)
  }

  case class Count(
      query: Query,
      cf: SumOrAvgCf = ConsolidationFunction.Avg,
      offset: Duration = Duration.ZERO) extends AggregateFunction {

    override def eval(context: EvalContext, data: List[TimeSeries]): ResultSet = {
      val filtered = data.filter(t => query.matches(t.tags)).map { t =>
        TimeSeries(t.tags, t.label, t.data.mapValues(v => if (v.isNaN) Double.NaN else 1.0))
      }
      val aggr = if (filtered.isEmpty) TimeSeries.noData(query, context.step) else {
        val tags = commonTags(filtered.head.tags)
        val t = TimeSeries.aggregate(filtered.iterator, context.start, context.end, this)
        TimeSeries(tags, TimeSeries.toLabel(tags), t.data)
      }
      val rs = consolidate(context.step, List(aggr))
      ResultSet(this, rs, context.state)
    }

    override def withConsolidation(f: ConsolidationFunction): AggregateFunction = f match {
      case v: SumOrAvgCf => copy(cf = v)
      case v             => Consolidation(copy(query = query, offset= offset), v)
    }

    override def withOffset(d: Duration): Count = copy(offset = d)

    def labelString: String = s"count(${query.labelString})"

    override def exprString: String = {
      if (cf == ConsolidationFunction.Avg) s"$query,:count" else s"$query,:count,$cf"
    }

    def apply(v1: Double, v2: Double): Double = Math.addNaN(v1, v2)
  }

  case class Min(query: Query, offset: Duration = Duration.ZERO) extends AggregateFunction {
    def cf: ConsolidationFunction = ConsolidationFunction.Min

    override def withConsolidation(f: ConsolidationFunction): AggregateFunction = {
      if (f == ConsolidationFunction.Min) this else Consolidation(this, f)
    }

    override def withOffset(d: Duration): Min = copy(offset = d)

    def labelString: String = s"min(${query.labelString})"

    override def exprString: String = s"$query,:min"

    def apply(v1: Double, v2: Double): Double = Math.minNaN(v1, v2)
  }

  case class Max(query: Query, offset: Duration = Duration.ZERO) extends AggregateFunction {
    def cf: ConsolidationFunction = ConsolidationFunction.Max

    override def withConsolidation(f: ConsolidationFunction): AggregateFunction = {
      if (f == ConsolidationFunction.Max) this else Consolidation(this, f)
    }

    override def withOffset(d: Duration): Max = copy(offset = d)

    def labelString: String = s"max(${query.labelString})"

    override def exprString: String = s"$query,:max"

    def apply(v1: Double, v2: Double): Double = Math.maxNaN(v1, v2)
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

    def labelString: String = af.labelString

    override def exprString: String = s"$af,$cf"
    def apply(v1: Double, v2: Double): Double = af(v1, v2)
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

    override def exprString: String = s"$af,(,${keys.mkString(",")},),:by"

    override def eval(context: EvalContext, data: List[TimeSeries]): ResultSet = {
      val ks = Query.exactKeys(query) ++ keys
      val groups = data
        .filter(t => query.matches(t.tags))
        .groupBy(t => keyString(t.tags))
        .filter(_._1 != null)
        .toList
      val sorted = groups.sortWith(_._1 < _._1)
      val newData = sorted.flatMap {
        case (null, _) => Nil
        case (k, Nil)  => List(TimeSeries.noData(query, context.step))
        case (k, ts)   =>
          val tags = ts.head.tags.filter(e => ks.contains(e._1))
          af.eval(context, ts).data.map { t =>
            TimeSeries(tags, k, t.data)
          }
      }
      val rs = consolidate(context.step, newData)
      ResultSet(this, rs, context.state)
    }
  }

  case class Head(expr: DataExpr, n: Int) extends DataExpr {
    require(n > 0, "number of matches must be > 0")

    override def query: Query = expr.query
    override def cf: ConsolidationFunction = expr.cf
    override def offset: Duration = expr.offset

    override def exprString: String = s"$expr,$n,:head"

    override def eval(context: EvalContext, data: List[TimeSeries]): ResultSet = {
      val rs = expr.eval(context, data)
      ResultSet(this, rs.data.take(n), rs.state)
    }
  }
}
