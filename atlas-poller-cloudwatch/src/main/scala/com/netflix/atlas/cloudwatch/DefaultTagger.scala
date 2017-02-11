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
package com.netflix.atlas.cloudwatch

import com.amazonaws.services.cloudwatch.model.Dimension
import com.typesafe.config.Config

/**
  * Tag datapoints with a set of common tags. Also allows mapping cloudwatch
  * dimension names to names that match some other convention. For example,
  * the cloudwatch dimension name AutoScalingGroupName corresponds to nf.asg
  * and we would rather have a single key in use for common concepts.
  */
class DefaultTagger(config: Config) extends Tagger {
  import scala.collection.JavaConverters._

  private val mappings: Map[String, String] = config.getConfigList("mappings")
    .asScala
    .map(c => c.getString("name") -> c.getString("alias"))
    .toMap

  private val commonTags: Map[String, String] = config.getConfigList("common-tags")
    .asScala
    .map(c => c.getString("key") -> c.getString("value"))
    .toMap

  private def toTag(dimension: Dimension): (String, String) = {
    mappings.getOrElse(dimension.getName, dimension.getName) -> dimension.getValue
  }

  override def apply(dimensions: List[Dimension]): Map[String, String] = {
    commonTags ++ dimensions.map(toTag).toMap
  }
}

