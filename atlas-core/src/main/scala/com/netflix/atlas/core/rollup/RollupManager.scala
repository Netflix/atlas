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
package com.netflix.atlas.core.rollup

import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.rollup.RollupManager._
import com.netflix.atlas.core.util.BoundedPriorityBuffer
import com.netflix.atlas.core.util.CardinalityEstimator
import com.typesafe.config.Config

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

trait RollupManager {
  protected val totalEstimator: CardinalityEstimator = CardinalityEstimator.newSyncEstimator()
  def total: Long = totalEstimator.cardinality

  def update(tags: Map[String, String]): Map[String, String] = {
    update(tags, TaggedItem.computeId(tags))
  }

  def update(tags: Map[String, String], id: AnyRef): Map[String, String] = {
    update(tags, id)
  }
  def cardinality(values: List[String]): Long
  def cardinality(values: List[String], tag: String): Long
  def topk(n: Int): AnyRef
}

class RollupStatsInner(val rollupConfig: RollupConfig, val level: Int) extends RollupManager {
  private val children = new ConcurrentHashMap[String, RollupManager]
  // Note: For now a dropped value should not be added even if a slot gets freed up, because data
  // may be incomplete.
  private val droppedValues: java.util.Set[String] = ConcurrentHashMap.newKeySet()

  override def update(
    tags: Map[String, String],
    id: AnyRef
  ): Map[String, String] = {
    val key = rollupConfig.keys(level)
    val value = tags.getOrElse(key, rollupConfig.getPrefixConfig(level).defaultValue)
    val conf = rollupConfig.getPrefixConfig(level)

    def hitLimit: Boolean = {
      conf.totalLimit > 0 && (total >= conf.totalLimit || children.size() >= conf.valueLimit)
    }

    val newTags = {
      if (droppedValues.contains(value)) {
        null
      } else if (!children.containsKey(value) && hitLimit) {
        // Don't drop for existing value even if limit is hit - it can be an existing time series
        droppedValues.add(value)
        null
      } else {
        children.computeIfAbsent(value, _ => createChildNode()).update(tags, id)
      }
    }
    //TODO no need for level 0
    totalEstimator.update(id)

    newTags
  }

  private def createChildNode() = {
    if (level == rollupConfig.keys.length - 1) {
      new RollupStatsLeaf(rollupConfig)
    } else {
      new RollupStatsInner(rollupConfig, level + 1)
    }
  }

  override def cardinality(values: List[String]): Long = {
    require(values != null, "Values can not be null")
    val maxNumValues = rollupConfig.keys.length - level
    require(
      values.length <= maxNumValues,
      s"number of values can not exceed $maxNumValues for level $level"
    )

    values match {
      case Nil => total
      case value :: rest =>
        val rollupManager: RollupManager = children.get(value)
        if (rollupManager == null) {
          0
        } else {
          rollupManager.cardinality(rest)
        }
    }
  }

  override def cardinality(values: List[String], tag: String): Long = {
    require(tag != null, "tag can not be null")
    val numValues = rollupConfig.keys.length - level
    require(
      values != null && values.length == numValues,
      s"number of values must be $numValues for level $level"
    )

    val rollupManager: RollupManager = children.get(values.head)

    if (rollupManager == null) {
      0
    } else {
      rollupManager.cardinality(values.drop(1), tag)
    }
  }

  override def topk(k: Int): AnyRef = {
    val buffer = new BoundedPriorityBuffer[java.util.Map.Entry[String, RollupManager]](
      k,
      Ordering.by(_.getValue.total)
    )
    children.entrySet().forEach(buffer.add)
    val key = rollupConfig.getPrefixConfig(level).key
    val list = buffer.toOrderedList
      .map(e => (s"$key: ${e.getKey}", e.getValue.topk(k)))
    Map("total_ts" -> total, s"num_of_$key" -> children.size(), s"top_$k" -> list)
  }
}

class RollupStatsLeaf(val rollupConfig: RollupConfig) extends RollupManager {

  private val tagCardinality = new ConcurrentHashMap[String, CardinalityEstimator]

  // Note: tags traversed twice - one for rollup check, the other for actual update or rollup
  override def update(
    tags: Map[String, String],
    id: AnyRef
  ): Map[String, String] = {
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
      if (!rollupConfig.isPrefixKey(key)) {
        val estimator = tagCardinality.get(key)
        if (estimator != null && estimator.cardinality >= rollupConfig.tagValueLimit) {
          result = key :: result
        }
      }
    }
    result
  }

  private def updateCardinalityStats(tags: Map[String, String], id: AnyRef): Unit = {
    totalEstimator.update(id)
    tags.foreachEntry { (key, value) =>
      // Size of defined prefix keys should be small, so array contains is efficient
      if (!rollupConfig.isPrefixKey(key)) {
        tagCardinality
          .computeIfAbsent(key, _ => CardinalityEstimator.newSyncEstimator())
          .update(value)
      }
    }
  }

  private def rollup(tags: Map[String, String], rollupKeys: Seq[String]): Map[String, String] = {
    tags.map {
      case (k, v) =>
        if (rollupKeys.contains(k))
          (k, RollupValue)
        else
          (k, v)
    }
  }

  override def cardinality(values: List[String]): Long = total

  override def cardinality(values: List[String], tag: String): Long = {
    require(tag != null, "tag can not be null")
    val estimator = tagCardinality.get(tag)
    if (estimator == null) 0 else estimator.cardinality
  }

  override def topk(k: Int): AnyRef = {
    val topTags = tagCardinality.asScala
      .map(t => (t._1, t._2.cardinality))
      .toList
      .sortBy(-_._2)
      .take(k)
      .mkString(",")

    s"total: $total, tags: $topTags"
  }
}

case class RollupConfig(
  prefixConfigs: Array[PrefixConfig],
  tagValueLimit: Int
) {
  require(prefixConfigs.length > 0)

  val keys: Array[String] = prefixConfigs.map(_.key)

  def getPrefixConfig(level: Int): PrefixConfig = {
    prefixConfigs(level)
  }

  def isPrefixKey(key: String): Boolean = {
    keys.contains(key)
  }
}

case class PrefixConfig(
  key: String,
  valueLimit: Int,
  totalLimit: Int
) {
  val defaultValue = s"unknown:$key"
}

object RollupManager {

  def newInstance(config: Config): RollupManager = {
    new RollupStatsInner(load(config), 0)
  }

  private def load(config: Config): RollupConfig = {
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
    }.toArray

    RollupConfig(configs, conf.getInt("tag-value-limit"))
  }

  val RollupValue = "_auto_rollup_"
}
