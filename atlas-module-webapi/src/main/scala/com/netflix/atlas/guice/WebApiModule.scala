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
package com.netflix.atlas.guice

import com.google.inject.AbstractModule
import com.netflix.atlas.core.db.Database
import com.netflix.atlas.webapi.DatabaseProvider
import com.netflix.iep.guice.LifecycleModule

/**
  * Configures the database needed for the webapi.
  */
final class WebApiModule extends AbstractModule {
  override def configure(): Unit = {
    install(new LifecycleModule)
    bind(classOf[Database]).toProvider(classOf[DatabaseProvider])
  }

  override def equals(obj: Any): Boolean = {
    obj != null && getClass.equals(obj.getClass)
  }

  override def hashCode(): Int = getClass.hashCode()
}
