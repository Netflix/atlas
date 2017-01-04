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
package com.netflix.atlas.webapi

import java.util.concurrent.TimeUnit

import com.netflix.atlas.chart.GraphEngine
import com.netflix.atlas.config.ConfigManager
import com.netflix.atlas.core.db.Database
import com.netflix.atlas.core.model.DefaultSettings
import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.validation.Rule
import com.typesafe.config.Config

object ApiSettings extends ApiSettings(ConfigManager.current)

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
    import scala.collection.JavaConversions._
    config.getStringList("graph.engines").toList.map { cname =>
      val cls = Class.forName(cname)
      cls.newInstance().asInstanceOf[GraphEngine]
    }
  }

  def graphVocabulary: Vocabulary = {
    config.getString("graph.vocabulary") match {
      case "default" => StyleVocabulary
      case cls       => Class.forName(cls).newInstance().asInstanceOf[Vocabulary]
    }
  }

  def maxDatapointAge: Long = config.getDuration("publish.max-age", TimeUnit.MILLISECONDS)

  def validationRules: List[Rule] = Rule.load(config.getConfigList("publish.rules"))

  def excludedWords: Set[String] = {
    import scala.collection.JavaConversions._
    config.getStringList("expr.complete.excluded-words").toSet
  }
}
