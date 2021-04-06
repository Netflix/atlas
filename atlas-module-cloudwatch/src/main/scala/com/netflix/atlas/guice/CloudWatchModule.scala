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
package com.netflix.atlas.guice

import javax.inject.Singleton
import com.google.inject.AbstractModule
import com.google.inject.Provides
import com.netflix.atlas.akka.AkkaModule
import com.netflix.iep.aws2.AwsClientFactory
import com.netflix.iep.aws2.AwsModule
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient

/**
  * Configures the binding for the cloudwatch client and poller.
  */
final class CloudWatchModule extends AbstractModule {

  override def configure(): Unit = {
    install(new AkkaModule)
    install(new AwsModule)
  }

  @Provides
  @Singleton
  protected def provideCloudWatchClient(factory: AwsClientFactory): CloudWatchClient = {
    factory.newInstance(classOf[CloudWatchClient])
  }

  override def equals(obj: Any): Boolean = {
    obj != null && getClass.equals(obj.getClass)
  }

  override def hashCode(): Int = getClass.hashCode()
}
