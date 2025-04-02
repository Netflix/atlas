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
package com.netflix.atlas.eval.model

import com.netflix.atlas.core.model.BinaryOp
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.DataExpr.AggregateFunction
import com.netflix.atlas.core.model.DataExpr.All
import com.netflix.atlas.core.model.DataExpr.GroupBy
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.util.Math
import com.netflix.atlas.core.util.RefDoubleHashMap
import com.netflix.spectator.api.Counter
import com.netflix.spectator.api.Registry

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
  *     Indicates whether it is an actual data point or a synthetic data point generated
  *     from a heartbeat.
  * @param tags
  *     Tags associated with the datapoint.
  * @param value
  *     Value for the datapoint.
  * @param samples
  *     Optional set of event samples associated with the message. Typically used when
  *     mapping events into a count with a few sample messages.
  */
case class AggrDatapoint(
  timestamp: Long,
  step: Long,
  expr: DataExpr,
  source: String,
  tags: Map[String, String],
  value: Double,
  samples: List[List[Any]] = Nil
) {

  /**
    * Converts this value to a time series type that can be used for the final evaluation
    * phase.
    */
  def toTimeSeries: TimeSeries = Datapoint(tags, timestamp, value, step)

  /** Check if it is a heartbeat datapoint. */
  def isHeartbeat: Boolean = source == "heartbeat"
}

object AggrDatapoint {

  private val aggrTagKey = "atlas.aggr"

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
    * Common settings for aggregators.
    *
    * @param maxInputDatapoints
    *     Limit for the number of input datapoints.
    * @param maxIntermediateDatapoints
    *     Limit for the number of intermediate datapoints.
    * @param registry
    *     Registry used for reporting metrics related to the aggregation behavior.
    */
  case class AggregatorSettings(
    maxInputDatapoints: Int,
    maxIntermediateDatapoints: Int,
    registry: Registry
  ) {

    /**
      * Counter for tracking number of datapoints that are dropped due to exceeding the
      * configured limits.
      */
    val droppedCounter: Counter =
      registry.counter("atlas.eval.datapoints", "id", "dropped-datapoints-limit-exceeded")
  }

  /**
    * Base trait for an aggregator that can efficiently combine the datapoints as they
    * arrive using the aggregation function for the data expression associated with the
    * datapoint. The caller should ensure that all datapoints passed to a given aggregator
    * instance have the same data expression.
    */
  abstract class Aggregator(settings: AggregatorSettings) {

    var numInputDatapoints = 0
    var numIntermediateDatapoints = 0

    protected[this] def checkLimits: Boolean = {
      val datapointsLimitExceeded = limitExceeded
      if (datapointsLimitExceeded) {
        settings.droppedCounter.increment()
      }
      datapointsLimitExceeded
    }

    /** Returns true if any of the configured limits have been exceeded. */
    def limitExceeded: Boolean = {
      numInputDatapoints >= settings.maxInputDatapoints ||
      numIntermediateDatapoints >= settings.maxIntermediateDatapoints
    }

    /**
      * Add datapoint to the aggregate. It may get dropped if configured limits have
      * been exceeded.
      */
    def aggregate(datapoint: AggrDatapoint): Aggregator

    /** Final set of aggregate datapoints. */
    def datapoints: List[AggrDatapoint]
  }

  /**
    * Aggregator for the simple base types: sum, min, max, and count. Note for count
    * the values need to be transformed to NaN or 1 prior to using the default operation
    * on DataExpr.Count of sum.
    */
  private class SimpleAggregator(
    init: AggrDatapoint,
    op: (Double, Double) => Double,
    settings: AggregatorSettings
  ) extends Aggregator(settings) {

    private var value = init.value
    numInputDatapoints += 1
    numIntermediateDatapoints = 1

    override def aggregate(datapoint: AggrDatapoint): Aggregator = {
      if (!checkLimits) {
        value = op(value, datapoint.value)
        numInputDatapoints += 1
      }
      this
    }

    override def datapoints: List[AggrDatapoint] = List(datapoint)

    def datapoint: AggrDatapoint = init.copy(value = value)
  }

  /**
    * Aggregator for the sum or count when used with gauges. In cases where data is going
    * to the aggregator service, there can be duplicates of the gauge values. To get a
    * correct sum as if it was from a single aggregator service instance, this will compute
    * the max for a given `atlas.aggr` key and then sum the final results.
    */
  private class GaugeSumAggregator(
    init: AggrDatapoint,
    op: (Double, Double) => Double,
    settings: AggregatorSettings
  ) extends Aggregator(settings) {

    private val maxValues = new RefDoubleHashMap[String]
    numIntermediateDatapoints = 1
    aggregate(init)

    override def aggregate(datapoint: AggrDatapoint): Aggregator = {
      if (!checkLimits) {
        val aggrKey = datapoint.tags.getOrElse(aggrTagKey, "unknown")
        maxValues.max(aggrKey, datapoint.value)
        numInputDatapoints += 1
      }
      this
    }

    override def datapoints: List[AggrDatapoint] = List(datapoint)

    def datapoint: AggrDatapoint = {
      val tags = init.tags - aggrTagKey
      var sum = 0.0
      maxValues.foreach { (_, v) => sum = op(sum, v) }
      init.copy(tags = tags, value = sum)
    }
  }

  /**
    * Group the datapoints by the tags and maintain a simple aggregator per distinct tag
    * set.
    */
  private class GroupByAggregator(settings: AggregatorSettings) extends Aggregator(settings) {

    private val aggregators =
      scala.collection.mutable.HashMap.empty[Map[String, String], Aggregator]

    private def newAggregator(datapoint: AggrDatapoint): Aggregator = {
      datapoint.expr match {
        case GroupBy(af: DataExpr.Sum, _) if datapoint.tags.contains(aggrTagKey) =>
          new GaugeSumAggregator(datapoint, aggrOp(af), settings)
        case GroupBy(af: DataExpr.Count, _) if datapoint.tags.contains(aggrTagKey) =>
          new GaugeSumAggregator(datapoint, aggrOp(af), settings)
        case GroupBy(af: AggregateFunction, _) =>
          new SimpleAggregator(datapoint, aggrOp(af), settings)
        case _ =>
          throw new IllegalArgumentException("datapoint is not for a grouped expression")
      }
    }

    override def aggregate(datapoint: AggrDatapoint): Aggregator = {
      if (!checkLimits) {
        numInputDatapoints += 1
        val tags = datapoint.tags - aggrTagKey
        aggregators.get(tags) match {
          case Some(aggr) =>
            aggr.aggregate(datapoint)
          case None =>
            numIntermediateDatapoints += 1
            aggregators.put(tags, newAggregator(datapoint))
        }
      }
      this
    }

    override def datapoints: List[AggrDatapoint] = {
      val builder = List.newBuilder[AggrDatapoint]
      aggregators.foreachEntry { (_, aggr) =>
        aggr.datapoints match {
          case d :: Nil => builder.addOne(d)
          case ds       => builder.addAll(ds)
        }
      }
      builder.result()
    }
  }

  /**
    * Do not perform aggregation. Keep track of all datapoints that have been received.
    */
  private class AllAggregator(settings: AggregatorSettings) extends Aggregator(settings) {

    private var values = List.empty[AggrDatapoint]

    override def aggregate(datapoint: AggrDatapoint): Aggregator = {
      if (!checkLimits) {
        values = datapoint :: values
        numInputDatapoints += 1
        numIntermediateDatapoints += 1
      }
      this
    }

    override def datapoints: List[AggrDatapoint] = values
  }

  /**
    * Create a new aggregator instance initialized with the specified datapoint. The
    * datapoint will already be applied and should not get re-added to the aggregation.
    */
  def newAggregator(datapoint: AggrDatapoint, settings: AggregatorSettings): Aggregator = {
    datapoint.expr match {
      case af: DataExpr.Sum if datapoint.tags.contains(aggrTagKey) =>
        new GaugeSumAggregator(datapoint, aggrOp(af), settings)
      case af: DataExpr.Count if datapoint.tags.contains(aggrTagKey) =>
        new GaugeSumAggregator(datapoint, aggrOp(af), settings)
      case af: AggregateFunction =>
        new SimpleAggregator(datapoint, aggrOp(af), settings)
      case _: GroupBy =>
        new GroupByAggregator(settings).aggregate(datapoint)
      case _: All =>
        new AllAggregator(settings).aggregate(datapoint)
    }
  }

  /** Return a binary operation that matches the requested aggregate function behavior. */
  @scala.annotation.tailrec
  private def aggrOp(af: AggregateFunction): BinaryOp = af match {
    case _: DataExpr.Sum              => Math.addNaN
    case _: DataExpr.Count            => Math.addNaN
    case _: DataExpr.Min              => Math.minNaN
    case _: DataExpr.Max              => Math.maxNaN
    case DataExpr.Consolidation(f, _) => aggrOp(f)
  }

  /**
    * Aggregate intermediate aggregates from each source to get the final aggregate for
    * a given expression. All values are expected to be for the same data expression.
    */
  def aggregate(values: List[AggrDatapoint], settings: AggregatorSettings): Option[Aggregator] = {
    if (values.isEmpty) Option.empty
    else {
      val aggr = newAggregator(values.head, settings)
      val aggregator = values.tail
        .foldLeft(aggr) { (acc, d) =>
          acc.aggregate(d)
        }
      Some(aggregator)
    }
  }
}
