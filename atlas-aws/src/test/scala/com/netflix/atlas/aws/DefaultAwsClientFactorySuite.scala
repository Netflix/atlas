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
package com.netflix.atlas.aws

import java.net.InetAddress

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.netflix.atlas.config.ConfigManager
import org.scalatest.FunSuite

class DefaultAwsClientFactorySuite extends FunSuite {

  import scala.collection.JavaConversions._

  val config = ConfigManager.current

  // Double check that the endpoints can be resolved, helps catch silly mistakes and typos
  val endpoints = config.getConfig("atlas.aws.endpoint").entrySet.toList
  endpoints.sortWith(_.getKey < _.getKey).foreach { endpoint =>
    val service = endpoint.getKey
      test(s"resolve: $service") {
        val host = endpoint.getValue.unwrapped.asInstanceOf[String]
        InetAddress.getByName(host)
      }
  }

  // Try a few key services
  val clientClasses = List(
    classOf[com.amazonaws.services.cloudwatch.AmazonCloudWatchClient],
    classOf[com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClient],
    classOf[com.amazonaws.services.ec2.AmazonEC2Client],
    classOf[com.amazonaws.services.ec2.AmazonEC2AsyncClient],
    classOf[com.amazonaws.services.s3.AmazonS3Client]
  )
  clientClasses.foreach { c =>
    val credentials = new DefaultAWSCredentialsProviderChain
    val factory = new DefaultAwsClientFactory(credentials, config.getConfig("atlas.aws"))
    test(s"newInstance: ${c.getSimpleName}") {
      val client = factory.newInstance(c)
      client.shutdown()
    }
  }
}
