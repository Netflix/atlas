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

import java.util.Date

import com.amazonaws.services.cloudwatch.model.Datapoint
import com.amazonaws.services.cloudwatch.model.StandardUnit
import com.netflix.atlas.core.model.Query
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class MetricDefinitionSuite extends FunSuite {

  private val meta = MetricMetadata(
    MetricCategory("AWS/ELB", 60, Nil, Nil, Query.True),
    null,
    Nil
  )

  test("bad config") {
    val cfg = ConfigFactory.empty()
    intercept[ConfigException] {
      MetricCategory.fromConfig(cfg)
    }
  }

  test("config with no tags") {
    val cfg = ConfigFactory.parseString(
      """
        |name = "RequestCount"
        |alias = "aws.elb.requests"
        |conversion = "sum,rate"
      """.stripMargin)

    val definitions = MetricDefinition.fromConfig(cfg)
    assert(definitions.size === 1)
    assert(definitions.head.name === "RequestCount")
    assert(definitions.head.alias === "aws.elb.requests")
  }

  test("config for timer") {
    val cfg = ConfigFactory.parseString(
      """
        |name = "Latency"
        |alias = "aws.elb.latency"
        |conversion = "timer"
      """.stripMargin)

    val definitions = MetricDefinition.fromConfig(cfg)
    assert(definitions.size === 3)
    assert(definitions.map(_.tags("statistic")).toSet === Set("totalTime", "count", "max"))

    val dp = new Datapoint()
      .withMaximum(6.0)
      .withMinimum(0.0)
      .withSum(600.0)
      .withSampleCount(1000.0)
      .withUnit(StandardUnit.Seconds)
      .withTimestamp(new Date)

    definitions.find(_.tags("statistic") == "totalTime").foreach { d =>
      val m = meta.copy(definition = d)
      assert(m.convert(dp) === 10.0)
    }

    definitions.find(_.tags("statistic") == "count").foreach { d =>
      val m = meta.copy(definition = d)
      assert(m.convert(dp) === 1000.0 / 60.0)
    }

    definitions.find(_.tags("statistic") == "max").foreach { d =>
      val m = meta.copy(definition = d)
      assert(m.convert(dp) === 6.0)
    }
  }

  test("config for timer-millis no unit") {
    val cfg = ConfigFactory.parseString(
      """
        |name = "Latency"
        |alias = "aws.elb.latency"
        |conversion = "timer-millis"
      """.stripMargin)

    val definitions = MetricDefinition.fromConfig(cfg)
    assert(definitions.size === 3)
    assert(definitions.map(_.tags("statistic")).toSet === Set("totalTime", "count", "max"))

    val dp = new Datapoint()
      .withMaximum(6.0)
      .withMinimum(0.0)
      .withSum(600.0)
      .withSampleCount(1000.0)
      .withUnit(StandardUnit.None)
      .withTimestamp(new Date)

    definitions.find(_.tags("statistic") == "totalTime").foreach { d =>
      val m = meta.copy(definition = d)
      assert(m.convert(dp) === 10.0 / 1000.0)
    }

    definitions.find(_.tags("statistic") == "count").foreach { d =>
      val m = meta.copy(definition = d)
      assert(m.convert(dp) === 1000.0 / 60.0)
    }

    definitions.find(_.tags("statistic") == "max").foreach { d =>
      val m = meta.copy(definition = d)
      assert(m.convert(dp) === 6.0 / 1000.0)
    }
  }

  test("config for timer-millis correct unit") {
    val cfg = ConfigFactory.parseString(
      """
        |name = "Latency"
        |alias = "aws.elb.latency"
        |conversion = "timer-millis"
      """.stripMargin)

    val definitions = MetricDefinition.fromConfig(cfg)
    assert(definitions.size === 3)
    assert(definitions.map(_.tags("statistic")).toSet === Set("totalTime", "count", "max"))

    val dp = new Datapoint()
      .withMaximum(6.0)
      .withMinimum(0.0)
      .withSum(600.0)
      .withSampleCount(1000.0)
      .withUnit(StandardUnit.Milliseconds)
      .withTimestamp(new Date)

    definitions.find(_.tags("statistic") == "totalTime").foreach { d =>
      val m = meta.copy(definition = d)
      assert(m.convert(dp) === 10.0 / 1000.0)
    }

    definitions.find(_.tags("statistic") == "count").foreach { d =>
      val m = meta.copy(definition = d)
      assert(m.convert(dp) === 1000.0 / 60.0)
    }

    definitions.find(_.tags("statistic") == "max").foreach { d =>
      val m = meta.copy(definition = d)
      assert(m.convert(dp) === 6.0 / 1000.0)
    }
  }

  test("config for dist-summary") {
    val cfg = ConfigFactory.parseString(
      """
        |name = "Latency"
        |alias = "aws.elb.latency"
        |conversion = "dist-summary"
      """.stripMargin)

    val definitions = MetricDefinition.fromConfig(cfg)
    assert(definitions.size === 3)
    assert(definitions.map(_.tags("statistic")).toSet === Set("totalAmount", "count", "max"))

    val dp = new Datapoint()
      .withMaximum(6.0)
      .withMinimum(0.0)
      .withSum(600.0)
      .withSampleCount(1000.0)
      .withUnit(StandardUnit.Bytes)
      .withTimestamp(new Date)

    definitions.find(_.tags("statistic") == "totalAmount").foreach { d =>
      val m = meta.copy(definition = d)
      assert(m.convert(dp) === 10.0)
    }

    definitions.find(_.tags("statistic") == "count").foreach { d =>
      val m = meta.copy(definition = d)
      assert(m.convert(dp) === 1000.0 / 60.0)
    }

    definitions.find(_.tags("statistic") == "max").foreach { d =>
      val m = meta.copy(definition = d)
      assert(m.convert(dp) === 6.0)
    }
  }

}
