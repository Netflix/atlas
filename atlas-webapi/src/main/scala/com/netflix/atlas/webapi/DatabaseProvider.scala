/*
 * Copyright 2014-2018 Netflix, Inc.
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
package com.netflix.atlas.webapi

import java.lang.reflect.Type
import javax.inject.Inject
import javax.inject.Provider
import javax.inject.Singleton

import com.netflix.atlas.core.db.Database
import com.netflix.iep.service.ClassFactory
import com.typesafe.config.Config

/**
  * Created by brharrington on 7/20/16.
  */
@Singleton
class DatabaseProvider @Inject() (config: Config, classFactory: ClassFactory)
  extends Provider[Database] {

  private val db = {
    import scala.compat.java8.FunctionConverters._
    val cfg = config.getConfig("atlas.core.db")
    val cls = cfg.getString("class")
    val overrides = Map[Type, AnyRef](classOf[Config] -> cfg).withDefaultValue(null)
    classFactory.newInstance[Database](cls, overrides.asJava)
  }

  override def get(): Database = db
}
