/*
 * Copyright 2014-2016 Netflix, Inc.
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
package com.netflix.atlas.akka

import javax.inject.Inject
import javax.inject.Provider
import javax.inject.Singleton

import akka.actor.ActorSystem
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config

/** Provider for getting an instance of the actor system. */
@Singleton
class ActorSystemProvider @Inject() (config: Config, registry: Registry)
  extends Provider[ActorSystem] {

  private val name = config.getString("atlas.akka.name")
  private val system = ActorSystem(name, config)

  override def get(): ActorSystem = system
}
