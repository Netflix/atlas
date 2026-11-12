/*
 * Copyright 2014-2026 Netflix, Inc.
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

import com.netflix.atlas.pekko.testkit.MUnitRouteSuite

import java.io.StringReader
import java.util.Properties
import org.apache.pekko.http.scaladsl.model.StatusCodes.*
import org.apache.pekko.http.scaladsl.testkit.RouteTestTimeout
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigObject

class ConfigApiSuite extends MUnitRouteSuite {

  import scala.concurrent.duration.*

  private implicit val routeTestTimeout: RouteTestTimeout = RouteTestTimeout(5.second)

  private val sysConfig = ConfigFactory.load()

  // System properties for the JVM running the tests could have values that get redacted, so
  // the expected config is the redacted version.
  private val redactedSysConfig = ConfigApi.redact(sysConfig)
  private val endpoint = new ConfigApi(sysConfig, system)

  test("/config") {
    Get("/api/v2/config") ~> endpoint.routes ~> check {
      val config = ConfigFactory.parseString(responseAs[String])
      assertEquals(redactedSysConfig, config)
    }
  }

  test("/config/") {
    Get("/api/v2/config/") ~> endpoint.routes ~> check {
      val config = ConfigFactory.parseString(responseAs[String])
      assertEquals(redactedSysConfig, config)
    }
  }

  test("/config/java") {
    Get("/api/v2/config/java") ~> endpoint.routes ~> check {
      val config = ConfigFactory.parseString(responseAs[String])
      assertEquals(redactedSysConfig.getConfig("java"), config)
    }
  }

  test("/config/os.arch") {
    import scala.jdk.CollectionConverters.*
    Get("/api/v2/config/os.arch") ~> endpoint.routes ~> check {
      val config = ConfigFactory.parseString(responseAs[String])
      val v = redactedSysConfig.getString("os.arch")
      val expected = ConfigFactory.parseMap(Map("value" -> v).asJava)
      assertEquals(expected, config)
    }
  }

  test("/config format hocon") {
    Get("/api/v2/config?format=hocon") ~> endpoint.routes ~> check {
      val config = ConfigFactory.parseString(responseAs[String])
      assertEquals(redactedSysConfig, config)
    }
  }

  test("/config format json") {
    Get("/api/v2/config?format=json") ~> endpoint.routes ~> check {
      val config = ConfigFactory.parseString(responseAs[String])
      assertEquals(redactedSysConfig, config)
    }
  }

  test("/config format properties") {
    Get("/api/v2/config?format=properties") ~> endpoint.routes ~> check {
      import scala.jdk.CollectionConverters.*
      val props = new Properties
      props.load(new StringReader(responseAs[String]))
      val config = ConfigFactory.parseProperties(props)

      // The quoting for keys seems to get messed with somewhere between Properties and Config
      // conversions. Not considered important enough to mess with right now so ignoring for the
      // test case...
      def normalize(c: Config): Map[String, String] = {
        c.entrySet.asScala
          .filter(!_.getKey.contains("\""))
          .map(t => t.getKey -> s"${t.getValue.unwrapped}")
          .toMap
      }

      val expected = normalize(redactedSysConfig)
      val actual = normalize(config)
      assertEquals(expected, actual)
    }
  }

  test("/config bad format") {
    Get("/api/v2/config?format=foo") ~> endpoint.routes ~> check {
      assertEquals(response.status, BadRequest)
    }
  }

  test("/config/foo") {
    Get("/api/v2/config/foo") ~> endpoint.routes ~> check {
      assertEquals(response.status, NotFound)
    }
  }

  private def redactedEndpoint: ConfigApi = {
    val extra = ConfigFactory.parseString(
      """
        |db {
        |  url = "jdbc:postgresql://localhost/atlas"
        |  user = "atlas"
        |  Password = "hunter2"
        |  api-token = "abc123"
        |}
        |client.secret = "s3"
        |client.id = "visible"
        |""".stripMargin
    )
    new ConfigApi(extra.withFallback(sysConfig), system)
  }

  test("/config redacts values for keys that look like secrets") {
    Get("/api/v2/config") ~> redactedEndpoint.routes ~> check {
      val config = ConfigFactory.parseString(responseAs[String])
      assertEquals(config.getString("db.Password"), Redaction.placeholder)
      assertEquals(config.getString("db.api-token"), Redaction.placeholder)
      assertEquals(config.getString("client.secret"), Redaction.placeholder)
      assertEquals(config.getString("db.url"), "jdbc:postgresql://localhost/atlas")
      assertEquals(config.getString("db.user"), "atlas")
      assertEquals(config.getString("client.id"), "visible")
    }
  }

  test("/config/db redacts values for keys that look like secrets") {
    Get("/api/v2/config/db") ~> redactedEndpoint.routes ~> check {
      val config = ConfigFactory.parseString(responseAs[String])
      assertEquals(config.getString("Password"), Redaction.placeholder)
      assertEquals(config.getString("user"), "atlas")
    }
  }

  test("/config/db.Password redacts the value") {
    Get("/api/v2/config/db.Password") ~> redactedEndpoint.routes ~> check {
      val config = ConfigFactory.parseString(responseAs[String])
      assertEquals(config.getString("value"), Redaction.placeholder)
    }
  }

  test("/config format properties redacts values for keys that look like secrets") {
    Get("/api/v2/config?format=properties") ~> redactedEndpoint.routes ~> check {
      val body = responseAs[String]
      assert(!body.contains("hunter2"))
      assert(!body.contains("abc123"))
    }
  }

  test("/config format hocon redacts values for keys that look like secrets") {
    Get("/api/v2/config?format=hocon") ~> redactedEndpoint.routes ~> check {
      assert(!responseAs[String].contains("hunter2"))
    }
  }

  test("redact patterns can be changed") {
    val cfg = ConfigFactory
      .parseString("""atlas.pekko.config-api.redact-key-patterns = ["user"]""")
      .withFallback(sysConfig)
    val endpoint = new ConfigApi(cfg.withFallback(ConfigFactory.parseString("db.user = a")), system)
    Get("/api/v2/config/db.user") ~> endpoint.routes ~> check {
      val config = ConfigFactory.parseString(responseAs[String])
      assertEquals(config.getString("value"), Redaction.placeholder)
    }
  }

  test("redact patterns can be empty") {
    val cfg = ConfigFactory
      .parseString("atlas.pekko.config-api.redact-key-patterns = []")
      .withFallback(sysConfig)
    val endpoint = new ConfigApi(cfg.withFallback(ConfigFactory.parseString("db.pass = a")), system)
    Get("/api/v2/config/db.pass") ~> endpoint.routes ~> check {
      val config = ConfigFactory.parseString(responseAs[String])
      assertEquals(config.getString("value"), "a")
    }
  }

  private def redact(str: String): Config = {
    ConfigApi.redact(ConfigFactory.parseString(str).withFallback(sysConfig))
  }

  test("redact: objects in lists") {
    val config = redact("""
        |servers = [
        |  {host = "a", password = "hunter2"},
        |  [{api-token = "abc123"}]
        |]
        |""".stripMargin)
    val server = config.getList("servers").get(0).asInstanceOf[ConfigObject].toConfig
    assertEquals(server.getString("host"), "a")
    assertEquals(server.getString("password"), Redaction.placeholder)
    assert(!config.root.render().contains("hunter2"))
    assert(!config.root.render().contains("abc123"))
  }

  test("redact: list for sensitive key") {
    val config = redact("""passwords = ["a", "b"]""")
    assertEquals(config.getString("passwords"), Redaction.placeholder)
  }

  test("redact: object for sensitive key is searched") {
    val config = redact("""
        |aws.credentials {
        |  role-arn = "arn:aws:iam::123:role/foo"
        |  secret-key = "s3"
        |}
        |""".stripMargin)
    assertEquals(config.getString("aws.credentials.role-arn"), "arn:aws:iam::123:role/foo")
    assertEquals(config.getString("aws.credentials.secret-key"), Redaction.placeholder)
  }

  test("redact: only key name is matched, not full path") {
    val config = redact("""token-service.host = "localhost"""")
    assertEquals(config.getString("token-service.host"), "localhost")
  }

  test("redact: number for sensitive key") {
    val config = redact("db.password = 123456")
    assertEquals(config.getString("db.password"), Redaction.placeholder)
  }

  test("redact: boolean for sensitive key") {
    val config = redact("db.password-enabled = true")
    assert(config.getBoolean("db.password-enabled"))
  }

  test("redact: quoted keys") {
    val config = redact(""""a.b".secret = "s3"""")
    assertEquals(config.getString("\"a.b\".secret"), Redaction.placeholder)
  }

  test("redact: uri user info") {
    val config = redact("""db.url = "postgresql://atlas:hunter2@localhost:5432/atlas"""")
    val expected = s"postgresql://atlas:${Redaction.placeholder}@localhost:5432/atlas"
    assertEquals(config.getString("db.url"), expected)
  }

  test("redact: uri user info without password") {
    val config = redact("""db.url = "postgresql://atlas@localhost/atlas"""")
    assertEquals(config.getString("db.url"), "postgresql://atlas@localhost/atlas")
  }

  test("redact: uri query parameters") {
    val config = redact(
      """db.url = "jdbc:postgresql://localhost/atlas?user=atlas&password=hunter2&ssl=true""""
    )
    val expected =
      s"jdbc:postgresql://localhost/atlas?user=atlas&password=${Redaction.placeholder}&ssl=true"
    assertEquals(config.getString("db.url"), expected)
  }

  test("redact: patterns are trimmed and empty patterns ignored") {
    val cfg = ConfigFactory
      .parseString(
        """
          |atlas.pekko.config-api.redact-key-patterns = [" Password ", ""]
          |db.password = "hunter2"
          |db.user = "atlas"
          |""".stripMargin
      )
      .withFallback(sysConfig)
    val config = ConfigApi.redact(cfg)
    assertEquals(config.getString("db.password"), Redaction.placeholder)
    assertEquals(config.getString("db.user"), "atlas")
  }
}
