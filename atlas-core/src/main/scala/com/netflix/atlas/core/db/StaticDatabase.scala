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
package com.netflix.atlas.core.db

import com.netflix.atlas.core.model.DefaultSettings
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.FunctionTimeSeq
import com.netflix.atlas.core.model.TimeSeries
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
  * Simple database with a predefined set of time series.
  */
class StaticDatabase(config: Config)
    extends SimpleStaticDatabase(DataSet.get(config.getString("dataset")), config)

object StaticDatabase {

  /** Create a simple database with a range of fixed integer values. Mostly used for testing. */
  def range(s: Int, e: Int): Database = {
    val len = e.toString.length
    val ts = (s to e).map { i =>
      val tagsBuilder = Map.newBuilder[String, String]
      tagsBuilder += "name"  -> s"%0${len}d".format(i)
      tagsBuilder += "class" -> (if (i % 2 == 1) "odd" else "even")
      if (probablyPrime(i))
        tagsBuilder += "prime" -> "probably"
      val seq = new FunctionTimeSeq(DsType.Gauge, DefaultSettings.stepSize, _ => i)
      TimeSeries(tagsBuilder.result(), seq)
    }
    new SimpleStaticDatabase(ts.toList, config)
  }

  /** Generate a database with some synthetic data used for demos and examples. */
  def demo: Database = {
    new SimpleStaticDatabase(DataSet.staticAlertSet, config)
  }

  private def config: Config = ConfigFactory.load().getConfig("atlas.core.db")

  private def probablyPrime(v: Int): Boolean = BigInt(v).isProbablePrime(100)
}
