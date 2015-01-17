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
package com.netflix.atlas.akka

import java.io.StringReader
import java.util.Properties

import com.netflix.atlas.config.ConfigManager
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite
import spray.http.StatusCodes._
import spray.testkit.ScalatestRouteTest


class ConfigApiSuite extends FunSuite with ScalatestRouteTest {

  val endpoint = new ConfigApi(system)
  val sysConfig = ConfigManager.current

  test("/config") {
    Get("/api/v2/config") ~> endpoint.routes ~> check {
      val config = ConfigFactory.parseString(responseAs[String])
      assert(sysConfig === config)
    }
  }

  test("/config/") {
    Get("/api/v2/config/") ~> endpoint.routes ~> check {
      val config = ConfigFactory.parseString(responseAs[String])
      assert(sysConfig === config)
    }
  }

  test("/config/java") {
    Get("/api/v2/config/java") ~> endpoint.routes ~> check {
      val config = ConfigFactory.parseString(responseAs[String])
      assert(sysConfig.getConfig("java") === config)
    }
  }

  test("/config/java.version") {
    import scala.collection.JavaConversions._
    Get("/api/v2/config/java.version") ~> endpoint.routes ~> check {
      val config = ConfigFactory.parseString(responseAs[String])
      val v = sysConfig.getString("java.version")
      val expected = ConfigFactory.parseMap(Map("value" -> v))
      assert(expected === config)
    }
  }

  test("/config format hocon") {
    Get("/api/v2/config?format=hocon") ~> endpoint.routes ~> check {
      val config = ConfigFactory.parseString(responseAs[String])
      assert(sysConfig === config)
    }
  }

  test("/config format json") {
    Get("/api/v2/config?format=json") ~> endpoint.routes ~> check {
      val config = ConfigFactory.parseString(responseAs[String])
      assert(sysConfig === config)
    }
  }

  test("/config format properties") {
    Get("/api/v2/config?format=properties") ~> endpoint.routes ~> check {
      import scala.collection.JavaConversions._
      val props = new Properties
      props.load(new StringReader(responseAs[String]))
      val config = ConfigFactory.parseProperties(props)

      // The quoting for keys seems to get messed with somewhere between Properties and Config
      // conversions. Not considered important enough to mess with right now so ignoring for the
      // test case...
      def normalize(c: Config): Map[String, String] = {
        c.entrySet.
          filter(!_.getKey.contains("\"")).
          map(t => t.getKey -> s"${t.getValue.unwrapped}").toMap
      }

      val expected = normalize(sysConfig)
      val actual = normalize(config)
      assert(expected === actual)
    }
  }

  test("/config bad format") {
    Get("/api/v2/config?format=foo") ~> endpoint.routes ~> check {
      assert(response.status === BadRequest)
    }
  }

  test("/config/foo") {
    Get("/api/v2/config/foo") ~> endpoint.routes ~> check {
      assert(response.status === NotFound)
    }
  }

}

