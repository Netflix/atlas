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
package com.netflix.atlas.standalone

import java.io.File
import com.netflix.iep.config.ConfigManager
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.springframework.context.annotation.AnnotationConfigApplicationContext

/**
  * Provides a simple way to start up a standalone server. Usage:
  *
  * ```
  * $ java -jar atlas.jar config1.conf config2.conf
  * ```
  */
object Main extends StrictLogging {

  private var app: com.netflix.iep.spring.Main = _

  private def loadAdditionalConfigFiles(files: Array[String]): Unit = {
    files.foreach { f =>
      logger.info(s"loading config file: $f")
      val file = new File(f)
      val c =
        if (file.exists())
          ConfigFactory.parseFileAnySyntax(file)
        else
          ConfigFactory.parseResourcesAnySyntax(f)
      ConfigManager.dynamicConfigManager.setOverrideConfig(c)
    }
  }

  def main(args: Array[String]): Unit = {
    try {
      loadAdditionalConfigFiles(if (args.nonEmpty) args else Array("static.conf"))
      val context = new AnnotationConfigApplicationContext()
      context.scan("com.netflix")
      context.registerBean(classOf[Config], () => ConfigManager.dynamicConfig())
      app = com.netflix.iep.spring.Main.run(args, context)
    } catch {
      case t: Throwable =>
        logger.error("server failed to start, shutting down", t)
        System.exit(1)
    }
  }

  def shutdown(): Unit = {
    app.close()
  }
}
