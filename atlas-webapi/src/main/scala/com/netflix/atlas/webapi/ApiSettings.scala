/*
 * Copyright 2015 Netflix, Inc.
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
import com.typesafe.config.Config

object ApiSettings {

  private def root = ConfigManager.current
  private def config = root.getConfig("atlas.webapi")

  def newDbInstance: Database = {
    val db = root.getConfig("atlas.core.db")
    val name = db.getString("class")
    val cls = Class.forName(name)
    cls.getConstructor(classOf[Config]).newInstance(db).asInstanceOf[Database]
  }

  def port: Int = config.getInt("main.port")

  def maxTagLimit: Int = config.getInt("tags.max-limit")

  def stepSize: Long = config.getDuration("graph.step", TimeUnit.MILLISECONDS)

  def startTime: String = config.getString("graph.start-time")

  def endTime: String = config.getString("graph.end-time")

  def timezone: String = config.getString("graph.timezone")

  def width: Int = config.getInt("graph.width")

  def height: Int = config.getInt("graph.height")

  def palette: String = config.getString("graph.palette")

  def maxDatapoints: Int = config.getInt("graph.max-datapoints")

  def engines: List[GraphEngine] = {
    import scala.collection.JavaConversions._
    config.getStringList("graph.engines").toList.map { cname =>
      val cls = Class.forName(cname)
      cls.newInstance().asInstanceOf[GraphEngine]
    }
  }
}
