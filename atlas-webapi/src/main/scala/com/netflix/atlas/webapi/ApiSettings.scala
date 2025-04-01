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
package com.netflix.atlas.webapi

import java.util.concurrent.TimeUnit

import com.netflix.atlas.chart.GraphEngine
import com.netflix.atlas.core.db.Database
import com.netflix.atlas.core.model.CustomVocabulary
import com.netflix.atlas.core.model.DefaultSettings
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.validation.Rule
import com.netflix.iep.config.ConfigManager
import com.typesafe.config.Config

object ApiSettings extends ApiSettings(ConfigManager.dynamicConfig())

class ApiSettings(root: => Config) {

  private def config = root.getConfig("atlas.webapi")

  def newDbInstance: Database = {
    val db = root.getConfig("atlas.core.db")
    val name = db.getString("class")
    val cls = Class.forName(name)
    cls.getConstructor(classOf[Config]).newInstance(db).asInstanceOf[Database]
  }

  def port: Int = config.getInt("main.port")

  def maxTagLimit: Int = config.getInt("tags.max-limit")

  def stepSize: Long = DefaultSettings.stepSize

  def startTime: String = config.getString("graph.start-time")

  def endTime: String = config.getString("graph.end-time")

  def timezone: String = config.getString("graph.timezone")

  def width: Int = config.getInt("graph.width")

  def height: Int = config.getInt("graph.height")

  def palette: String = config.getString("graph.palette")

  def metadataEnabled: Boolean = config.getBoolean("graph.png-metadata-enabled")

  def maxDatapoints: Int = config.getInt("graph.max-datapoints")

  def engines: List[GraphEngine] = {
    import scala.jdk.CollectionConverters.*
    config.getStringList("graph.engines").asScala.toList.map { cname =>
      newInstance[GraphEngine](cname)
    }
  }

  def graphVocabulary: Vocabulary = {
    config.getString("graph.vocabulary") match {
      case "default" => new CustomVocabulary(root)
      case cls       => newInstance[Vocabulary](cls)
    }
  }

  def fetchChunkSize: Int = config.getInt("fetch.chunk-size")

  def maxPermittedTags: Int = config.getInt("publish.max-permitted-tags")

  def maxDatapointAge: Long = config.getDuration("publish.max-age", TimeUnit.MILLISECONDS)

  def validationRules: List[Rule] = Rule.load(config.getConfigList("publish.rules"))

  def excludedWords: Set[String] = {
    import scala.jdk.CollectionConverters.*
    config.getStringList("expr.complete.excluded-words").asScala.toSet
  }

  private def newInstance[T](cls: String): T = {
    Class.forName(cls).getDeclaredConstructor().newInstance().asInstanceOf[T]
  }
}
