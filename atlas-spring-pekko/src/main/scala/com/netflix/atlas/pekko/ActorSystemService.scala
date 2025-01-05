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
package com.netflix.atlas.pekko

import org.apache.pekko.actor.ActorSystem
import com.netflix.iep.service.AbstractService
import com.typesafe.config.Config

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ActorSystemService(config: Config) extends AbstractService {

  val system: ActorSystem = ActorSystem(config.getString("atlas.pekko.name"), config)

  override def startImpl(): Unit = {}

  override def stopImpl(): Unit = {
    Await.ready(system.terminate(), Duration.Inf)
  }
}
