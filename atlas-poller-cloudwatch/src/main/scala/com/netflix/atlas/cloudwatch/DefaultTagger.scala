/*
 * Copyright 2014-2018 Netflix, Inc.
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
package com.netflix.atlas.cloudwatch

import com.amazonaws.services.cloudwatch.model.Dimension
import com.typesafe.config.Config

import scala.util.matching.Regex

/**
  * Tag datapoints with a set of common tags. Also allows mapping cloudwatch
  * dimension names to names that match some other convention. For example,
  * the cloudwatch dimension name AutoScalingGroupName corresponds to nf.asg
  * and we would rather have a single key in use for common concepts.
  */
class DefaultTagger(config: Config) extends Tagger {
  import scala.collection.JavaConverters._

  private val extractors: Map[String, Seq[(Regex, Option[String])]] = config
    .getConfigList("extractors")
    .asScala
    .map(c => {
      val patterns = c
        .getConfigList("patterns")
        .asScala
        .map(cl => {
          val alias = if (cl.hasPath("alias")) Some(cl.getString("alias")) else None
          cl.getString("pattern").r -> alias
        })
      c.getString("name") -> patterns
    })
    .toMap

  private val mappings: Map[String, String] = config
    .getConfigList("mappings")
    .asScala
    .map(c => c.getString("name") -> c.getString("alias"))
    .toMap

  private val commonTags: Map[String, String] = config
    .getConfigList("common-tags")
    .asScala
    .map(c => c.getString("key") -> c.getString("value"))
    .toMap

  private def toTag(dimension: Dimension): (String, String) = {
    val cwName = dimension.getName
    val cwValue = dimension.getValue

    val optionalExtractor = extractors.get(cwName)
    val extracted = optionalExtractor.flatMap { patterns =>
      patterns.flatMap {
        case (regex, alias) =>
          val extractedValue = regex.findFirstMatchIn(cwValue).map { matches =>
            if (matches.groupCount > 0) matches.group(1) else cwValue
          }
          extractedValue.map(v => alias.getOrElse(cwName) -> v)
      }.headOption
    }

    extracted.getOrElse(mappings.getOrElse(cwName, cwName) -> cwValue)
  }

  override def apply(dimensions: List[Dimension]): Map[String, String] = {
    commonTags ++ dimensions.map(toTag).toMap
  }
}
