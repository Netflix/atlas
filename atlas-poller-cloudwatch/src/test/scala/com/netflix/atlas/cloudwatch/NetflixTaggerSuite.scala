/*
 * Copyright 2014-2021 Netflix, Inc.
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

import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.services.cloudwatch.model.Dimension

class NetflixTaggerSuite extends AnyFunSuite {

  private val dimensions = List(
    Dimension.builder().name("AutoScalingGroupName").value("app_name-stack-detail-v001").build(),
    Dimension.builder().name("ClusterName").value("different_name-foo-bar-v002").build()
  )

  test("production config loads") {
    val cfg = ConfigFactory.parseResources("reference.conf").resolve()

    val tagger = CloudWatchPoller.getTagger(cfg)
    val tagged = tagger(
      List(
        Dimension.builder().name("aTag").value("aValue").build(),
        Dimension.builder().name("LinkedAccount").value("12345").build()
      )
    )
    assert(tagged.getOrElse("aTag", "fail") === "aValue")
    assert(tagged.getOrElse("aws.account", "fail") === "12345")
  }

  test("bad config") {
    val cfg = ConfigFactory.parseString("")
    intercept[ConfigException] {
      new NetflixTagger(cfg)
    }
  }

  test("extract tags using naming conventions") {
    val cfg = ConfigFactory.parseString("""
        |extractors = []
        |mappings = [
        |  {
        |    name = "AutoScalingGroupName"
        |    alias = "nf.asg"
        |  }
        |]
        |common-tags = []
        |netflix-keys = ["nf.asg"]
      """.stripMargin)

    val expected = Map(
      "nf.app"      -> "app_name",
      "nf.cluster"  -> "app_name-stack-detail",
      "nf.asg"      -> "app_name-stack-detail-v001",
      "nf.stack"    -> "stack",
      "ClusterName" -> "different_name-foo-bar-v002"
    )

    val tagger = new NetflixTagger(cfg)
    assert(tagger(dimensions) === expected)
  }

}
