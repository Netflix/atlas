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
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.pekko.http.scaladsl.model.StatusCodes

class ServiceDocSuite extends MUnitRouteSuite {

  private def config(overrides: String): Config = {
    ConfigFactory.parseString(overrides).withFallback(ConfigFactory.load()).resolve()
  }

  // Config for the document that is available in the test resources.
  private def enabled(title: String = "Test service guide"): Config = {
    config(s"""
        |atlas.pekko.service-doc.resource = "www/llms.txt"
        |atlas.pekko.service-doc.title = "$title"
      """.stripMargin)
  }

  test("disabled by default") {
    val doc = ServiceDoc(config(""))
    assertEquals(doc.resource, None)
    assertEquals(doc.linkHeader, None)
  }

  test("link header for available document") {
    val doc = ServiceDoc(enabled())
    assertEquals(doc.resource, Some("www/llms.txt"))
    assertEquals(
      doc.linkHeader.map(_.value),
      Some("</.well-known/llms.txt>; rel=service-doc; title=\"Test service guide\"")
    )
  }

  test("missing resource, no link header") {
    val doc = ServiceDoc(config("atlas.pekko.service-doc.resource = \"www/missing.txt\""))
    assertEquals(doc.resource, None)
    assertEquals(doc.linkHeader, None)
  }

  test("disabled, no link header") {
    val doc = ServiceDoc(config("atlas.pekko.service-doc.resource = \"\""))
    assertEquals(doc.resource, None)
    assertEquals(doc.linkHeader, None)
  }

  test("quotes and backslashes in the title are escaped") {
    val doc = ServiceDoc(enabled("""a \"b\" c\\d"""))
    assertEquals(
      doc.linkHeader.map(_.value),
      Some("</.well-known/llms.txt>; rel=service-doc; title=\"a \\\"b\\\" c\\\\d\"")
    )
  }

  test("control characters in the title are dropped") {
    val doc = ServiceDoc(enabled("""a\r\nX-Injected: yes"""))
    assertEquals(
      doc.linkHeader.map(_.value),
      Some("</.well-known/llms.txt>; rel=service-doc; title=\"aX-Injected: yes\"")
    )
  }

  test("empty title omits the parameter") {
    val doc = ServiceDoc(enabled(""))
    assertEquals(doc.linkHeader.map(_.value), Some("</.well-known/llms.txt>; rel=service-doc"))
  }

  test("document served at the well-known path") {
    val doc = ServiceDoc(enabled())
    Get(ServiceDoc.wellKnownPath) ~> doc.routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assert(responseAs[String].contains("Agent facing document"))
    }
  }

  test("document served at the alias path") {
    val doc = ServiceDoc(enabled())
    Get(ServiceDoc.aliasPath) ~> doc.routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assert(responseAs[String].contains("Agent facing document"))
    }
  }

  test("paths are not mapped if there is no document") {
    val doc = ServiceDoc(config("atlas.pekko.service-doc.resource = \"\""))
    Get(ServiceDoc.wellKnownPath) ~> doc.routes ~> check {
      assert(!handled)
    }
  }
}
