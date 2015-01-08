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

import com.amazonaws.AmazonWebServiceClient
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.typesafe.config.Config


class DefaultAwsClientFactory(credentials: AWSCredentialsProvider, config: Config)
    extends AwsClientFactory {

  private def configure[T <: AmazonWebServiceClient](key: String, client: T): T = {
    val region = config.getString("region")
    client.setEndpoint(config.getString(s"endpoint.$key.$region"))
    client
  }

  // Assumed to be the last part of the package name
  private def awsServiceName(c: Class[_]): String = {
    val pkg = c.getPackage.getName
    val pos = pkg.lastIndexOf(".")
    pkg.substring(pos + 1)
  }

  /**
   * Create a new instance of an AWS client. The client must have a constructor that takes a
   * credential provider and client configuration.
   *
   * @param c
   *     Class for the client to create.
   * @return
   *     New instance of the client.
   */
  def newInstance[T <: AmazonWebServiceClient](c: Class[T]): T = {
    val ctor = c.getConstructor(classOf[AWSCredentialsProvider], classOf[ClientConfiguration])
    val client = ctor.newInstance(credentials, AwsClientFactory.defaultClientConfig)
    configure(awsServiceName(c), client)
  }
}
