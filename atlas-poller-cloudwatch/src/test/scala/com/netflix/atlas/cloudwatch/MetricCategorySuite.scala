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

import com.netflix.atlas.core.model.Query
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class MetricCategorySuite extends FunSuite {
  test("bad config") {
    val cfg = ConfigFactory.empty()
    intercept[ConfigException] {
      MetricCategory.fromConfig(cfg)
    }
  }

  test("load from config") {
    val cfg = ConfigFactory.parseString(
      """
        |namespace = "AWS/ELB"
        |period = 1 m
        |dimensions = ["LoadBalancerName"]
        |metrics = [
        |  {
        |    name = "RequestCount"
        |    alias = "aws.elb.requests"
        |    conversion = "sum,rate"
        |  },
        |  {
        |    name = "HTTPCode_ELB_4XX"
        |    alias = "aws.elb.errors"
        |    conversion = "sum,rate"
        |    tags = [
        |      {
        |        key = "status"
        |        value = "4xx"
        |      }
        |    ]
        |  }
        |]
      """.stripMargin)

    val category = MetricCategory.fromConfig(cfg)
    assert(category.namespace === "AWS/ELB")
    assert(category.period === 60)
    assert(category.toListRequests.size === 2)
    assert(category.filter === Query.True)
  }

  test("config with filter") {
    val cfg = ConfigFactory.parseString(
      """
        |namespace = "AWS/ELB"
        |period = 1 m
        |dimensions = ["LoadBalancerName"]
        |metrics = [
        |  {
        |    name = "RequestCount"
        |    alias = "aws.elb.requests"
        |    conversion = "sum,rate"
        |  }
        |]
        |filter = "name,RequestCount,:eq"
      """.stripMargin)

    val category = MetricCategory.fromConfig(cfg)
    assert(category.filter === Query.Equal("name", "RequestCount"))
  }

  test("config with invalid filter") {
    val cfg = ConfigFactory.parseString(
      """
        |namespace = "AWS/ELB"
        |period = 1 m
        |dimensions = ["LoadBalancerName"]
        |metrics = [
        |  {
        |    name = "RequestCount"
        |    alias = "aws.elb.requests"
        |    conversion = "sum,rate"
        |  }
        |]
        |filter = "name,:invalid-command"
      """.stripMargin)

    intercept[IllegalStateException] {
      MetricCategory.fromConfig(cfg)
    }
  }
}
