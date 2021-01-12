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
import com.netflix.atlas.core.util.CardinalityEstimator

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.IteratorHasAsScala

/**
  * This class track cardinality stats by app, name, and other tags.
  */
class CardinalityStats {
  private val appToAppStats = new ConcurrentHashMap[String, AppStats]

  /**
    * Update cardinality stats with given tags and id.
    *
    * @param tags
    *     the time series tags
    * @param id
    *     the id calculated from tags, to avoid recalculation if available already
    */
  def update(tags: Map[String, String], id: AnyRef): Unit = {
    appToAppStats
      .computeIfAbsent(tags.getOrElse("nf.app", "unknown-app"), _ => new AppStats)
      .update(tags, id)
  }

  /**
    * Update cardinality stats with given tags.
    *
    * @param tags
    *     the time series tags
    */
  def update(tags: Map[String, String]): Unit = {
    update(tags, TaggedItem.computeId(tags))
  }

  /**
    * Iterator for app and cardinality tuples.
    *
    * @return
    *     iterator for app and cardinality tuples
    */
  def appCardinalityIterator(): Iterator[(String, Int)] = {
    appToAppStats
      .entrySet()
      .iterator()
      .asScala
      .map(entry => (entry.getKey, entry.getValue.cardinality))
  }

  /**
    * Iterator for app, name and cardinality tuples.
    *
    * @return
    *     iterator for app, name and cardinality tuples
    */
  def appNameCardinalityIterator(): Iterator[(String, String, Int)] = {
    appToAppStats
      .entrySet()
      .iterator()
      .asScala
      .flatMap(entry =>
        entry.getValue.nameCardinalityIterator
          .map(nameCardinality => (entry.getKey, nameCardinality._1, nameCardinality._2))
      )
  }

  /**
    * Iterator for app, name, tag and cardinality tuples.
    *
    * @return
    *     iterator for app, name, tag and cardinality tuples
    */
  def appNameTagCardinalityIterator(): Iterator[(String, String, String, Int)] = {
    appToAppStats
      .entrySet()
      .iterator()
      .asScala
      .flatMap(entry =>
        entry.getValue.nameTagCardinalityIterator
          .map(nameTagCardinality =>
            (entry.getKey, nameTagCardinality._1, nameTagCardinality._2, nameTagCardinality._3)
          )
      )
  }

  /**
    * Get cardinality for the given app, name and tag.
    *
    * @param app
    *     app
    * @param name
    *     name
    * @param tag
    *     tag
    * @return
    *     cardinality for given app, name and tag
    */
  def appNameTagCardinality(app: String, name: String, tag: String): Int = {
    require(tag != "name" && tag != "nf.app", "tag can not be name or nf.app")
    val appStats = appToAppStats.get(app)
    if (appStats == null) {
      0
    } else {
      appStats.cardinalityByNameTag(name, tag)
    }
  }

  /**
    * Get cardinality for the given app.
    *
    * @param app
    *     app
    * @return
    *     cardinality for the given app
    */
  def appCardinality(app: String): Int = {
    val appStats = appToAppStats.get(app)
    if (appStats == null) {
      0
    } else {
      appStats.cardinality
    }
  }

  /**
    * Get cardinality for the given app and name.
    *
    * @param app
    *   app
    * @param name
    *   name
    * @return
    *   cardinality for the given app and name
    */
  def appNameCardinality(app: String, name: String): Int = {
    val appStats = appToAppStats.get(app)
    if (appStats == null) {
      0
    } else {
      appStats.cardinalityByName(name)
    }
  }
}

private class AppStats {
  private val total = CardinalityEstimator.newEstimator()
  private val nameToNameStats = new ConcurrentHashMap[String, NameStats]

  def update(tags: Map[String, String], id: AnyRef): Unit = {
    total.update(id)
    nameToNameStats
      .computeIfAbsent(tags("name"), _ => new NameStats)
      .update(tags, id)
  }

  def cardinality: Int = {
    total.cardinality
  }

  def cardinalityByName(name: String): Int = {
    val nameStats = nameToNameStats.get(name)
    if (nameStats == null) {
      0
    } else {
      nameStats.cardinality
    }
  }

  def cardinalityByNameTag(name: String, tag: String): Int = {
    val nameStats = nameToNameStats.get(name)
    if (nameStats == null) {
      0
    } else {
      nameStats.tagCardinality(tag)
    }
  }

  def nameCardinalityIterator: Iterator[(String, Int)] = {
    nameToNameStats
      .entrySet()
      .iterator()
      .asScala
      .map(entry => (entry.getKey, entry.getValue.cardinality))
  }

  def nameTagCardinalityIterator: Iterator[(String, String, Int)] = {
    nameToNameStats
      .entrySet()
      .iterator()
      .asScala
      .flatMap(entry =>
        entry.getValue.tagCardinalityIterator
          .map(tagCardinality => (entry.getKey, tagCardinality._1, tagCardinality._2))
      )
  }
}

private class NameStats {

  private val total = CardinalityEstimator.newEstimator()
  private val tagToValueCardinality = new ConcurrentHashMap[String, CardinalityEstimator]

  def update(tags: Map[String, String], id: AnyRef): Unit = {
    total.update(id)
    tags.foreach {
      case (tag, value) =>
        if (tag != "nf.app" && tag != "name") {
          tagToValueCardinality
            .computeIfAbsent(tag, _ => CardinalityEstimator.newEstimator())
            .update(value)
        }
    }
  }

  def cardinality: Int = total.cardinality

  def tagCardinality(tag: String): Int = {
    val tagEst = tagToValueCardinality.get(tag)
    if (tagEst == null) {
      0
    } else {
      tagEst.cardinality
    }
  }

  def tagIterator: Iterator[String] = {
    tagToValueCardinality.keySet().iterator().asScala
  }

  def tagCardinalityIterator: Iterator[(String, Int)] = {
    tagToValueCardinality
      .entrySet()
      .iterator()
      .asScala
      .map(entry => (entry.getKey, entry.getValue.cardinality))
  }
}
