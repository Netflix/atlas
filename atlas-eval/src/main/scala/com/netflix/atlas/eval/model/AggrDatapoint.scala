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
package com.netflix.atlas.eval.model

import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.DataExpr.AggregateFunction
import com.netflix.atlas.core.model.DataExpr.All
import com.netflix.atlas.core.model.DataExpr.GroupBy
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.TimeSeries

/**
  * Datapoint for an aggregate data expression. This type is used for the intermediate
  * results during evaluation of an expression until we get the final aggregated value
  * for a given query.
  *
  * @param timestamp
  *     Timestamp for all values that contributed to the aggregate. It should already
  *     be normalized to the step interval for the data stream prior to aggregation
  *     taking place.
  * @param step
  *     Step size for the subscription. Datapoints should be received at this frequency.
  * @param expr
  *     Data expression associated with the value. This is needed if further aggregation
  *     is necessary and later for matching in the final evaluation phase.
  * @param source
  *     The source combined with the expression are used for deduping the intermediate
  *     aggregates. This can be ignored at the risk of some values being included in the
  *     final result multiple times.
  * @param tags
  *     Tags associated with the datapoint.
  * @param value
  *     Value for the datapoint.
  */
case class AggrDatapoint(
  timestamp: Long,
  step: Long,
  expr: DataExpr,
  source: String,
  tags: Map[String, String],
  value: Double
) {

  /** Identifier used for deduping intermediate aggregates. */
  def id: String = s"$source:$expr"

  /**
    * Converts this value to a time series type that can be used for the final evaluation
    * phase.
    */
  def toTimeSeries: TimeSeries = Datapoint(tags, timestamp, value, step)

  /** Check if it is a heartbeat datapoint. */
  def isHeartbeat: Boolean = source == "heartbeat"
}

object AggrDatapoint {

  /**
    * Creates a dummy datapoint passed along when a heartbeat message is received from the
    * lwcapi server. These are used to ensure regular messages are flowing into the time
    * grouping stage so it will flush even if there is no matching data for any of the
    * expressions being evaluated.
    */
  def heartbeat(timestamp: Long, step: Long): AggrDatapoint = {
    val t = timestamp / step * step
    AggrDatapoint(t, step, DataExpr.All(Query.False), "heartbeat", Map.empty, Double.NaN)
  }

  /**
    * Base trait for an aggregator that can efficiently combine the datapoints as they
    * arrive using the aggregation function for the data expression associated with the
    * datapoint. The caller should ensure that all datapoints passed to a given aggregator
    * instance have the same data expression.
    */
  trait Aggregator {
    protected[this] var rawDatapointCounter = 0
    def numRawDatapoints: Int = rawDatapointCounter
    def aggregate(datapoint: AggrDatapoint): Aggregator
    def datapoints: List[AggrDatapoint]
  }

  /**
    * Aggregator for the simple base types: sum, min, max, and count. Note for count
    * the values need to be transformed to NaN or 1 prior to using the default operation
    * on DataExpr.Count of sum.
    */
  private class SimpleAggregator(init: AggrDatapoint, op: (Double, Double) => Double)
      extends Aggregator {

    private var value = init.value
    rawDatapointCounter += 1

    override def aggregate(datapoint: AggrDatapoint): Aggregator = {
      value = op(value, datapoint.value)
      rawDatapointCounter += 1
      this
    }

    override def datapoints: List[AggrDatapoint] = List(datapoint)

    def datapoint: AggrDatapoint = init.copy(value = value)
  }

  /**
    * Group the datapoints by the tags and maintain a simple aggregator per distinct tag
    * set.
    */
  private class GroupByAggregator extends Aggregator {

    private val aggregators =
      scala.collection.mutable.AnyRefMap.empty[Map[String, String], SimpleAggregator]

    private def newAggregator(datapoint: AggrDatapoint): SimpleAggregator = {
      datapoint.expr match {
        case GroupBy(af: AggregateFunction, _) =>
          rawDatapointCounter += 1
          new SimpleAggregator(datapoint, af)
        case _ =>
          throw new IllegalArgumentException("datapoint is not for a grouped expression")
      }
    }

    override def aggregate(datapoint: AggrDatapoint): Aggregator = {
      aggregators.get(datapoint.tags) match {
        case Some(aggr) =>
          rawDatapointCounter += 1
          aggr.aggregate(datapoint)
        case None => aggregators.put(datapoint.tags, newAggregator(datapoint))
      }
      this
    }

    override def datapoints: List[AggrDatapoint] = {
      aggregators.values.map(_.datapoint).toList
    }
  }

  /**
    * Do not perform aggregation. Keep track of all datapoints that have been received.
    */
  private class AllAggregator extends Aggregator {
    private var values = List.empty[AggrDatapoint]

    override def aggregate(datapoint: AggrDatapoint): Aggregator = {
      values = datapoint :: values
      rawDatapointCounter += 1
      this
    }

    override def datapoints: List[AggrDatapoint] = values
  }

  /**
    * Create a new aggregator instance initialized with the specified datapoint. The
    * datapoint will already be applied and should not get re-added to the aggregation.
    */
  def newAggregator(datapoint: AggrDatapoint): Aggregator = {
    datapoint.expr match {
      case af: AggregateFunction => new SimpleAggregator(datapoint, af)
      case _: GroupBy            => (new GroupByAggregator).aggregate(datapoint)
      case _: All                => (new AllAggregator).aggregate(datapoint)
    }
  }

  /**
    * Aggregate intermediate aggregates from each source to get the final aggregate for
    * a given expression. All values are expected to be for the same data expression.
    */
  def aggregate(values: List[AggrDatapoint]): List[AggrDatapoint] = {
    if (values.isEmpty) Nil
    else {
      val vs = dedup(values)
      val aggr = newAggregator(vs.head)
      vs.tail
        .foldLeft(aggr) { (acc, d) =>
          acc.aggregate(d)
        }
        .datapoints
    }
  }

  /**
    * Dedup the values using the ids for each value. This will take into account the
    * group by keys such that values with different grouping keys will not be considered
    * as duplicates.
    */
  private def dedup(values: List[AggrDatapoint]): List[AggrDatapoint] = {
    values.groupBy(_.id).map(_._2.head).toList
  }
}
