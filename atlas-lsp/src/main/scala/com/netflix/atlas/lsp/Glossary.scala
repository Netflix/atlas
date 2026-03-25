/*
 * Copyright 2014-2026 Netflix, Inc.
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
package com.netflix.atlas.lsp

import com.netflix.atlas.json3.Json

case class Glossary(
  id: String,
  description: Option[String] = None,
  metrics: Map[String, MetricDef] = Map.empty,
  tagKeys: Map[String, TagKeyDef] = Map.empty,
  tagValues: Map[String, TagValueDef] = Map.empty
)

case class MetricDef(
  description: String,
  unit: Option[String] = None,
  category: Option[String] = None,
  link: Option[String] = None,
  `type`: Option[String] = None,
  tags: Map[String, MetricTagDef] = Map.empty
)

case class MetricTagDef(
  description: Option[String] = None,
  required: Option[Boolean] = None,
  values: Option[List[String]] = None,
  valuesType: Option[String] = None
)

case class TagKeyDef(
  description: String,
  link: Option[String] = None,
  category: Option[String] = None,
  values: Option[List[String]] = None,
  valuesType: Option[String] = None
)

case class TagValueDef(
  description: String,
  link: Option[String] = None
)

object Glossary {

  val empty: Glossary = Glossary(id = "empty")

  def load(resource: String): Glossary = {
    val stream = getClass.getClassLoader.getResourceAsStream(resource)
    if (stream == null) {
      throw new IllegalArgumentException(s"glossary resource not found: $resource")
    }
    try Json.decode[Glossary](stream)
    finally stream.close()
  }

  def merge(a: Glossary, b: Glossary): Glossary = {
    Glossary(
      id = b.id,
      description = b.description.orElse(a.description),
      metrics = mergeMetrics(a.metrics, b.metrics),
      tagKeys = mergeTagKeys(a.tagKeys, b.tagKeys),
      tagValues = a.tagValues ++ b.tagValues
    )
  }

  private def mergeMetrics(
    a: Map[String, MetricDef],
    b: Map[String, MetricDef]
  ): Map[String, MetricDef] = {
    val keys = a.keySet ++ b.keySet
    keys.iterator.map { k =>
      val merged = (a.get(k), b.get(k)) match {
        case (Some(ma), Some(mb)) =>
          MetricDef(
            description = mb.description,
            unit = mb.unit.orElse(ma.unit),
            category = mb.category.orElse(ma.category),
            link = mb.link.orElse(ma.link),
            `type` = mb.`type`.orElse(ma.`type`),
            tags = mergeMetricTags(ma.tags, mb.tags)
          )
        case (Some(m), None) => m
        case (None, Some(m)) => m
        case _               => throw new IllegalStateException
      }
      k -> merged
    }.toMap
  }

  private def mergeMetricTags(
    a: Map[String, MetricTagDef],
    b: Map[String, MetricTagDef]
  ): Map[String, MetricTagDef] = {
    val keys = a.keySet ++ b.keySet
    keys.iterator.map { k =>
      val merged = (a.get(k), b.get(k)) match {
        case (Some(ma), Some(mb)) =>
          MetricTagDef(
            description = mb.description.orElse(ma.description),
            required = mb.required.orElse(ma.required),
            values = mergeValues(ma.values, mb.values),
            valuesType = mergeValuesType(ma.valuesType, mb.valuesType)
          )
        case (Some(m), None) => m
        case (None, Some(m)) => m
        case _               => throw new IllegalStateException
      }
      k -> merged
    }.toMap
  }

  private def mergeTagKeys(
    a: Map[String, TagKeyDef],
    b: Map[String, TagKeyDef]
  ): Map[String, TagKeyDef] = {
    val keys = a.keySet ++ b.keySet
    keys.iterator.map { k =>
      val merged = (a.get(k), b.get(k)) match {
        case (Some(ma), Some(mb)) =>
          TagKeyDef(
            description = mb.description,
            link = mb.link.orElse(ma.link),
            category = mb.category.orElse(ma.category),
            values = mergeValues(ma.values, mb.values),
            valuesType = mergeValuesType(ma.valuesType, mb.valuesType)
          )
        case (Some(m), None) => m
        case (None, Some(m)) => m
        case _               => throw new IllegalStateException
      }
      k -> merged
    }.toMap
  }

  private def mergeValues(
    a: Option[List[String]],
    b: Option[List[String]]
  ): Option[List[String]] = {
    (a, b) match {
      case (Some(va), Some(vb)) => Some((va ++ vb).distinct)
      case _                    => a.orElse(b)
    }
  }

  private def mergeValuesType(a: Option[String], b: Option[String]): Option[String] = {
    (a, b) match {
      case (Some("examples"), _) => Some("examples")
      case (_, Some("examples")) => Some("examples")
      case (_, Some(v))          => Some(v)
      case (Some(v), None)       => Some(v)
      case (None, None)          => None
    }
  }
}
