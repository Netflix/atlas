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
import com.netflix.frigga.Names
import com.typesafe.config.Config

/**
  * Tag the datapoints using Frigga to extract app and cluster information based
  * on naming conventions used by Spinnaker and Asgard.
  */
class NetflixTagger(config: Config) extends DefaultTagger(config) {
  import scala.collection.JavaConverters._

  private val keys = config.getStringList("netflix-keys").asScala

  private def opt(k: String, s: String): Option[(String, String)] = {
    Option(s).filter(_ != "").map(v => k -> v)
  }

  override def apply(dimensions: List[Dimension]): Map[String, String] = {
    val baseTags = super.apply(dimensions)
    val extractedTags = keys.flatMap { k =>
      baseTags.get(k).map { v =>
        val name = Names.parseName(v)
        List(
          opt("nf.app",     name.getApp),
          opt("nf.cluster", name.getCluster),
          opt("nf.stack",   name.getStack)
        ).flatten
      }
    }
    extractedTags.flatten.toMap ++ baseTags
  }
}

