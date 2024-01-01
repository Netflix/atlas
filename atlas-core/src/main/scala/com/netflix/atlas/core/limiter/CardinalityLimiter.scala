/*
 * Copyright 2014-2024 Netflix, Inc.
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
package com.netflix.atlas.core.limiter

import com.netflix.atlas.core.limiter.CardinalityLimiter.*
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.util.BoundedPriorityBuffer
import com.netflix.atlas.core.util.CardinalityEstimator
import com.typesafe.config.Config

import java.util.concurrent.ConcurrentHashMap
import scala.collection.compat.immutable.ArraySeq
import scala.jdk.CollectionConverters.*

/**
  * A structure that tracks cardinality based on tag keys, and drop or rollup tags based on given
  * configuration.
  */
abstract class CardinalityLimiter(val limiterConfig: LimiterConfig) {

  protected val totalEstimator: CardinalityEstimator = CardinalityEstimator.newEstimator()

  /**
    * Update cardinality stats for the given tags.
    * @param tags
    *   tags to track
    * @param id
    *   id that represents the tags
    * @return
    *   - null if it should be dropped
    *   - updated tags if tag roll up happened
    *   - same object with input tags otherwise
    */
  def update(tags: Map[String, String], id: AnyRef): Map[String, String]

  /**
    * Default to [[com.netflix.atlas.core.model.TaggedItem.computeId]] to compute id.
    */
  def update(tags: Map[String, String]): Map[String, String] = {
    update(tags, TaggedItem.computeId(tags))
  }

  /**
    * Get total cardinality for distinct tag maps ever seen.
    */
  def cardinality: Long = totalEstimator.cardinality

  /**
    * Get cardinality for a given list of values of prefix keys.
    */
  def cardinalityBy(values: List[String]): Long

  /**
    * Get cardinality for a given list of values of prefix keys and a specific tag key.
    */
  def cardinalityBy(values: List[String], tagKey: String): Long

  /**
    * Check if a query can be served based on the cardinality stats and defined limits.
    */
  def canServe(query: Query): Boolean = canServe(QueryInfo(query, limiterConfig))

  private[limiter] def canServe(queryInfo: QueryInfo): Boolean

  /**
    * A convenient way get top k keys by cardinality at all levels, mainly used for debug/inspect.
    * @param k
    *   number of top keys
    * @return
    *   cardinality stats
    */
  def topK(k: Int): CardinalityStats
}

/**
  * Non-leaf node of the tree, tracks stats by prefix keys.
  */
private[limiter] class CardinalityLimiterInner(
  override val limiterConfig: LimiterConfig,
  val level: Int
) extends CardinalityLimiter(limiterConfig) {

  private val children = new ConcurrentHashMap[String, CardinalityLimiter]
  // Note: For now a dropped value should not be added even if a slot gets freed up, because data
  // may be incomplete.
  private val droppedValues: java.util.Set[String] = ConcurrentHashMap.newKeySet()

  override def update(
    tags: Map[String, String],
    id: AnyRef
  ): Map[String, String] = {
    val key = limiterConfig.prefixKeys(level)
    val value = tags.getOrElse(key, MissingKey)
    val newTags = {
      if (droppedValues.contains(value)) {
        null
      } else if (!children.containsKey(value) && reachLimit) {
        // Don't drop for existing value even if limit is hit - it can be an existing time series
        droppedValues.add(value)
        null
      } else {
        children.computeIfAbsent(value, _ => createChildNode()).update(tags, id)
      }
    }
    totalEstimator.update(id)
    newTags
  }

  private def reachLimit: Boolean = {
    val conf = limiterConfig.getPrefixConfig(level)
    conf.totalLimit > 0 && (cardinality >= conf.totalLimit || children.size() >= conf.valueLimit)
  }

  private def createChildNode() = {
    if (level == limiterConfig.prefixKeys.length - 1) {
      new CardinalityLimiterLeaf(limiterConfig)
    } else {
      new CardinalityLimiterInner(limiterConfig, level + 1)
    }
  }

  override def cardinalityBy(values: List[String]): Long = {
    require(values != null, "Values can not be null")
    val maxNumValues = limiterConfig.prefixKeys.length - level
    require(
      values.length <= maxNumValues,
      s"number of values can not exceed $maxNumValues for level $level"
    )

    values match {
      case Nil => cardinality
      case value :: rest =>
        val child = children.get(value)
        if (child == null) {
          0
        } else {
          child.cardinalityBy(rest)
        }
    }
  }

  override def cardinalityBy(values: List[String], tagKey: String): Long = {
    require(tagKey != null, "tag can not be null")
    val numValues = limiterConfig.prefixKeys.length - level
    require(
      values != null && values.length == numValues,
      s"number of values must be $numValues for level $level"
    )

    val limiter: CardinalityLimiter = children.get(values.head)

    if (limiter == null) {
      0
    } else {
      limiter.cardinalityBy(values.drop(1), tagKey)
    }
  }

  override def topK(k: Int): CardinalityStats = {
    val buffer = new BoundedPriorityBuffer[java.util.Map.Entry[String, CardinalityLimiter]](
      k,
      Ordering.by(_.getValue.cardinality)
    )
    children.entrySet().forEach(buffer.add)

    val key = limiterConfig.prefixKeys(level)
    val byValue = buffer.drainToOrderedList().map(e => (e.getKey, e.getValue.topK(k)))
    CardinalityStats(key, cardinality, byValue, Nil)
  }

  // Not in dropped set, and then check next level
  override def canServe(queryInfo: QueryInfo): Boolean = {
    val value = queryInfo.prefixValues(level)
    if (droppedValues.contains(value)) {
      false
    } else {
      val child = children.get(value)
      // OK if a value is never seen - it means no drop or rollup either
      child == null || child.canServe(queryInfo)
    }
  }
}

/**
  * Leaf-node of the tree, tracks stats for tags belongs to a specific value of last level of prefix
  * keys: total cardinality and value cardinality for non-prefix tag keys.
  */
private[limiter] class CardinalityLimiterLeaf(override val limiterConfig: LimiterConfig)
    extends CardinalityLimiter(limiterConfig) {

  private val tagCardinality = new ConcurrentHashMap[String, CardinalityEstimator]

  override def update(tags: Map[String, String], id: AnyRef): Map[String, String] = {
    val rollupKeys = checkRollup(tags)
    val newTags =
      if (rollupKeys == Nil) {
        // Only update if no rollup - it's a different time series after rollup
        tags
      } else {
        rollup(tags, rollupKeys)
      }
    updateCardinalityStats(tags, id)
    newTags
  }

  private def checkRollup(tags: Map[String, String]): List[String] = {
    var result: List[String] = Nil
    tags.foreachEntry { (key, _) =>
      if (!limiterConfig.isPrefixKey(key) && reachLimit(key)) {
        result = key :: result
      }
    }
    result
  }

  private def reachLimit(key: String): Boolean = {
    val estimator = tagCardinality.get(key)
    estimator != null && estimator.cardinality >= limiterConfig.tagValueLimit
  }

  private def updateCardinalityStats(tags: Map[String, String], id: AnyRef): Unit = {
    totalEstimator.update(id)
    tags.foreachEntry { (key, value) =>
      if (!limiterConfig.isPrefixKey(key)) {
        tagCardinality
          .computeIfAbsent(key, _ => CardinalityEstimator.newEstimator())
          .update(value)
      }
    }
  }

  private def rollup(tags: Map[String, String], rollupKeys: Seq[String]): Map[String, String] = {
    // Use tuple for iteration to minimize allocation
    tags.map { t =>
      if (rollupKeys.contains(t._1)) {
        (t._1, RollupValue)
      } else
        t
    }
  }

  override def cardinalityBy(values: List[String]): Long = cardinality

  override def cardinalityBy(values: List[String], tagKey: String): Long = {
    require(tagKey != null, "tag can not be null")
    val estimator = tagCardinality.get(tagKey)
    if (estimator == null) 0 else estimator.cardinality
  }

  override def topK(k: Int): CardinalityStats = {
    val topTags = tagCardinality.asScala
      .map(t => (t._1, t._2.cardinality))
      .toList
      .sortBy(-_._2)
      .take(k)
    CardinalityStats("_", cardinality, Nil, topTags)
  }

  override private[limiter] def canServe(queryInfo: QueryInfo): Boolean = {
    queryInfo.queryKeys.forall { !reachLimit(_) }
  }
}

object CardinalityLimiter {

  /**
    * Create a new instance of CardinalityLimiter.
    */
  def newInstance(config: Config): CardinalityLimiter = {
    new CardinalityLimiterInner(load(config), 0)
  }

  private def load(config: Config): LimiterConfig = {
    val conf = config.getConfig("atlas.auto-rollup")
    val configs = {
      conf
        .getConfigList("prefix")
        .asScala
        .map(c => {
          PrefixConfig(
            c.getString("key"),
            c.getInt("value-limit"),
            c.getInt("total-limit")
          )
        })
    }
    LimiterConfig(ArraySeq.from(configs), conf.getInt("tag-value-limit"))
  }

  val RollupValue = "_auto-rollup_"
  val MissingKey = "_missing-key_"
}

case class QueryInfo(query: Query, limiterConfig: LimiterConfig) {

  val prefixValues: Array[String] = genSearchPath()
  // Not needed if dropped early in search path
  lazy val queryKeys: Set[String] = Query.allKeys(query) -- limiterConfig.prefixKeys

  // Find the values of prefix keys to search through the tree, use default value if missing.
  private[limiter] def genSearchPath(): Array[String] = {
    val cnfList = Query.cnfList(query)
    val prefixKeys = limiterConfig.prefixKeys
    val path = Array.fill[String](prefixKeys.length)(MissingKey)
    cnfList.foreach {
      case Query.Equal(k, v) =>
        for (i <- prefixKeys.indices) {
          if (k == prefixKeys(i)) {
            path(i) = v
          }
        }
      case _ => // do nothing
    }
    path
  }
}

case class CardinalityStats(
  key: String,
  total: Long,
  cardinalityByValue: List[(String, CardinalityStats)],
  cardinalityByTag: List[(String, Long)]
)
