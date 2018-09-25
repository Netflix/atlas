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
package com.netflix.atlas.core.algorithm

import com.typesafe.config.Config
import com.typesafe.config.ConfigList
import com.typesafe.config.ConfigObject
import com.typesafe.config.ConfigValue
import com.typesafe.config.ConfigValueFactory

/**
  * Base trait for online algorithms used on time series.
  */
trait OnlineAlgorithm {

  /** Apply the next value from the input and return the computed value. */
  def next(v: Double): Double

  /** Reset the state of the algorithm. */
  def reset(): Unit

  /**
    * Capture the current state of the algorithm. It can be restored in a new instance
    * with the [OnlineAlgorithm#apply] method.
    */
  def state: Config
}

object OnlineAlgorithm {

  /**
    * Create a new instance initialized with the captured state from a previous instance
    * of an online algorithm.
    */
  def apply(config: Config): OnlineAlgorithm = {
    config.getString("type") match {
      case "delay"       => OnlineDelay(config)
      case "des"         => OnlineDes(config)
      case "ignore"      => OnlineIgnoreN(config)
      case "pipeline"    => Pipeline(config)
      case "rolling-min" => OnlineRollingMin(config)
      case "rolling-max" => OnlineRollingMax(config)
      case "sliding-des" => OnlineSlidingDes(config)
      case t             => throw new IllegalArgumentException(s"unknown type: '$t'")
    }
  }

  private[algorithm] def toConfig(state: Map[String, _]): Config = {
    toConfigObject(state).toConfig
  }

  private def toConfigValue(state: Any): ConfigValue = {
    state match {
      case m: Map[_, _]    => toConfigObject(m.asInstanceOf[Map[String, _]])
      case vs: Iterable[_] => toConfigList(vs)
      case vs: Array[_]    => toConfigList(vs.toList)
      case c: Config       => c.root()
      case v               => ConfigValueFactory.fromAnyRef(v)
    }
  }

  private def toConfigObject(state: Map[String, _]): ConfigObject = {
    import scala.collection.JavaConverters._
    ConfigValueFactory.fromMap(state.mapValues(toConfigValue).asJava)
  }

  private def toConfigList(state: Iterable[_]): ConfigList = {
    import scala.collection.JavaConverters._
    ConfigValueFactory.fromIterable(state.map(toConfigValue).asJava)
  }
}
