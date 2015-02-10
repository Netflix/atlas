/*
 * Copyright 2015 Netflix, Inc.
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

  def eval(context: EvalContext, data: Map[DataExpr, List[TimeSeries]]): ResultSet = {
    ResultSet(this, data.getOrElse(this, Nil), context.state)
  }

  override def toString: String = {
    if (offset.isZero) exprString else s"$exprString,$offset,:offset"
  }
}

object DataExpr {
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

    def withConsolidation(f: ConsolidationFunction): AggregateFunction

    override def eval(context: EvalContext, data: List[TimeSeries]): ResultSet = {
      val filtered = data.filter(t => query.matches(t.tags))
      val aggr = if (filtered.isEmpty) TimeSeries.noData(context.step) else {
        TimeSeries.aggregate(filtered.iterator, context.start, context.end, this)
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
      val aggr = TimeSeries.aggregate(filtered.iterator, context.start, context.end, this)
      val rs = consolidate(context.step, List(aggr))
      ResultSet(this, rs, context.state)
    }

    override def withConsolidation(f: ConsolidationFunction): AggregateFunction = f match {
      case v: SumOrAvgCf => copy(cf = v)
      case v             => Consolidation(copy(query = query, offset= offset), v)
    }

    override def withOffset(d: Duration): Count = copy(offset = d)

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
    override def exprString: String = s"$query,:min"
    def apply(v1: Double, v2: Double): Double = Math.minNaN(v1, v2)
  }

  case class Max(query: Query, offset: Duration = Duration.ZERO) extends AggregateFunction {
    def cf: ConsolidationFunction = ConsolidationFunction.Max

    override def withConsolidation(f: ConsolidationFunction): AggregateFunction = {
      if (f == ConsolidationFunction.Max) this else Consolidation(this, f)
    }

    override def withOffset(d: Duration): Max = copy(offset = d)
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

    def keyString(tags: Map[String, String]): String = {
      val builder = new StringBuilder
      keys.foreach { k =>
        val v = tags.get(k)
        if (v.isEmpty) return null
        builder.append(k).append("=").append(v.get).append(" ")
      }
      builder.toString
    }

    override def groupByKey(tags: Map[String, String]): Option[String] = Option(keyString(tags))

    override def exprString: String = s"$af,(,${keys.mkString(",")},),:by"

    override def eval(context: EvalContext, data: List[TimeSeries]): ResultSet = {
      val newData = data.groupBy(t => keyString(t.tags)).flatMap {
        case (null, _) => Nil
        case (k, ts) =>
          af.eval(context, ts).data.map { t =>
            TimeSeries(t.tags, s"(${k.trim})", t.data)
          }
      }
      val rs = consolidate(context.step, newData.toList)
      ResultSet(this, rs, context.state)
    }
  }
}
