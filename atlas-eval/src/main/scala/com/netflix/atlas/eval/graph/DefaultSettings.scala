/*
 * Copyright 2014-2019 Netflix, Inc.
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
package com.netflix.atlas.eval.graph

import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import akka.http.scaladsl.model.ContentType
import com.netflix.atlas.chart.GraphEngine
import com.netflix.atlas.core.model.CustomVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.Vocabulary
import com.typesafe.config.Config

/**
  * Default settings to use when rendering a graph image.
  *
  * @param root
  *     The full config object for the app. Primarily used for constructing
  *     any custom vocabulary that is needed for the interpreter.
  * @param config
  *     The specific config block for graph settings. This is typically under
  *     `atlas.eval.graph`.
  */
case class DefaultSettings(root: Config, config: Config) {

  /**
    * Default step size to use for the chart. This should typically match the primary step
    * size of the underlying storage.
    */
  val stepSize: Long = config.getDuration("step", TimeUnit.MILLISECONDS)

  /**
    * Default start time for the chart. This value should typically be relative to the
    * end time.
    */
  val startTime: String = config.getString("start-time")

  /** Default end time for the chart. This value should typically be relative to `now`. */
  val endTime: String = config.getString("end-time")

  /** Default time zone for the chart. */
  val timezone: String = config.getString("timezone")

  /** Default width for the chart. */
  val width: Int = config.getInt("width")

  /** Default height for the chart. */
  val height: Int = config.getInt("height")

  /** Default theme to use for the chart. */
  val theme: String = config.getString("theme")

  /** Should the uri and other graph metadata be encoded as text fields in the image? */
  val metadataEnabled: Boolean = config.getBoolean("png-metadata-enabled")

  /** Pattern to use for detecting if a user-agent is a web-browser. */
  val browserAgentPattern: Pattern = {
    Pattern.compile(config.getString("browser-agent-pattern"), Pattern.CASE_INSENSITIVE)
  }

  /** Maximum number of datapoints allowed for a line in a chart. */
  val maxDatapoints: Int = config.getInt("max-datapoints")

  /** Available engines for rendering a chart. */
  val engines: Map[String, GraphEngine] = {
    import scala.jdk.CollectionConverters._
    config
      .getStringList("engines")
      .asScala
      .toList
      .map { cname =>
        val e = newInstance[GraphEngine](cname)
        e.name -> e
      }
      .toMap
  }

  /** Content types for the various rendering options. */
  val contentTypes: Map[String, ContentType] = engines.map {
    case (k, e) =>
      k -> ContentType.parse(e.contentType).toOption.get
  }

  /** Vocabulary to use in the interpreter when evaluating a graph expression. */
  val graphVocabulary: Vocabulary = {
    config.getString("vocabulary") match {
      case "default" => new CustomVocabulary(root)
      case cls       => newInstance[Vocabulary](cls)
    }
  }

  /** Interpreter for the graph expressions. */
  val interpreter = Interpreter(graphVocabulary.allWords)

  private def newInstance[T](cls: String): T = {
    Class.forName(cls).getDeclaredConstructor().newInstance().asInstanceOf[T]
  }
}

object DefaultSettings {

  def apply(root: Config): DefaultSettings = {
    DefaultSettings(root, root.getConfig("atlas.eval.graph"))
  }
}
