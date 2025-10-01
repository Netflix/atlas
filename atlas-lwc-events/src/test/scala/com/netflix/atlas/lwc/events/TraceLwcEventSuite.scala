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
package com.netflix.atlas.lwc.events

import munit.FunSuite

class TraceLwcEventSuite extends FunSuite {

  test("tagValue: exists") {
    assertEquals(TraceLwcEvent.sampleTrace.tagValue("kind"), "SERVER")
    assertEquals(TraceLwcEvent.sampleTrace.tagValue("status"), "OK")
    assertEquals(TraceLwcEvent.sampleTrace.tagValue("app"), "a")
    assertEquals(TraceLwcEvent.sampleTrace.tagValue("root.app"), "a")
  }

  test("tagValue: missing") {
    assertEquals(TraceLwcEvent.sampleTrace.tagValue("parent.app"), null)
    assertEquals(TraceLwcEvent.sampleTrace.tagValue("foo"), null)
  }

  test("tagValue: foreach") {
    TraceLwcEvent.sampleTrace.foreach { event =>
      event.tagValue("app") match {
        case "a" => assertEquals(event.tagValue("parent.app"), null)
        case "b" => assertEquals(event.tagValue("parent.app"), "a")
        case "c" => assertEquals(event.tagValue("parent.app"), "a")
        case "d" => assertEquals(event.tagValue("parent.app"), "a")
        case "e" => assert(Set("b", "c").contains(event.tagValue("parent.app")))
        case "f" => assertEquals(event.tagValue("parent.app"), "b")
      }
    }
  }

  test("tagValue: multi-level parent") {
    TraceLwcEvent.sampleTrace.foreach { event =>
      event.tagValue("app") match {
        case "e" | "f" =>
          assertEquals(event.tagValue("parent.parent.app"), "a", event.toJson)
        case _ =>
          assertEquals(event.tagValue("parent.parent.app"), null, event.toJson)
      }
    }
  }

  private def rewrite(str: String): String = {
    val query = ExprUtils.parseDataExpr(str).query
    TraceLwcEvent.rewriteToParentQuery(query).toString
  }

  test("rewriteToParentQuery: no prefix") {
    List(":eq", ":re", ":reic", ":lt", ":le", ":gt", ":ge").foreach { op =>
      assertEquals(rewrite(s"app,foo,$op"), s"app,foo,$op")
    }
  }

  test("rewriteToParentQuery: parent depth 1") {
    List(":eq", ":re", ":reic", ":lt", ":le", ":gt", ":ge").foreach { op =>
      assertEquals(rewrite(s"parent.app,foo,$op"), s"app,foo,$op")
    }
  }

  test("rewriteToParentQuery: child depth 1") {
    List(":eq", ":re", ":reic", ":lt", ":le", ":gt", ":ge").foreach { op =>
      assertEquals(rewrite(s"child.app,foo,$op"), s"app,foo,$op")
    }
  }

  test("rewriteToParentQuery: local and child key") {
    assertEquals(
      rewrite("app,foo,:eq,child.app,bar,:eq,:and"),
      "parent.app,foo,:eq,app,bar,:eq,:and"
    )
  }

  test("rewriteToParentQuery: multi-level cancel out") {
    assertEquals(
      rewrite("app,foo,:eq,child.app,bar,:eq,:and,parent.child.child.parent.status,200,:eq,:and"),
      "parent.app,foo,:eq,app,bar,:eq,:and,parent.status,200,:eq,:and"
    )
  }
}
