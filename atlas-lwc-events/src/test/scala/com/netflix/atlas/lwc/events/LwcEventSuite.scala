/*
 * Copyright 2014-2023 Netflix, Inc.
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

import com.netflix.atlas.core.util.SortedTagMap
import munit.FunSuite

class LwcEventSuite extends FunSuite {

  import LwcEventSuite.*

  private val sampleSpan: TestEvent = {
    TestEvent(SortedTagMap("app" -> "www", "node" -> "i-123"), 42L)
  }

  private val sampleLwcEvent: LwcEvent = LwcEvent(sampleSpan, extractSpanValue(sampleSpan))

  test("tagValue: exists") {
    assertEquals(sampleLwcEvent.tagValue("app"), "www")
    assertEquals(sampleLwcEvent.tagValue("node"), "i-123")
  }

  test("tagValue: missing") {
    assertEquals(sampleLwcEvent.tagValue("foo"), null)
  }

  test("tagValue: wrong type") {
    assertEquals(sampleLwcEvent.tagValue("duration"), null)
  }

  test("extractValue: exists") {
    assertEquals(sampleLwcEvent.extractValue("app"), "www")
    assertEquals(sampleLwcEvent.extractValue("node"), "i-123")
    assertEquals(sampleLwcEvent.extractValue("duration"), 42L)
  }

  test("extractValue: missing") {
    assertEquals(sampleLwcEvent.extractValue("foo"), null)
  }

  test("toJson: raw event") {
    val expected = """{"tags":{"app":"www","node":"i-123"},"duration":42}"""
    assertEquals(sampleLwcEvent.toJson, expected)
  }

  test("toJson: row no columns") {
    val expected = """[]"""
    assertEquals(sampleLwcEvent.toJson(List.empty), expected)
  }

  test("toJson: row nested object") {
    val expected = """[42,{"app":"www","node":"i-123"}]"""
    assertEquals(sampleLwcEvent.toJson(List("duration", "tags")), expected)
  }

  test("toJson: row simple") {
    val expected = """[42,"www"]"""
    assertEquals(sampleLwcEvent.toJson(List("duration", "app")), expected)
  }
}

object LwcEventSuite {

  case class TestEvent(tags: Map[String, String], duration: Long)

  def extractSpanValue(span: TestEvent)(key: String): Any = {
    key match {
      case "tags"     => span.tags
      case "duration" => span.duration
      case k          => span.tags.getOrElse(k, null)
    }
  }
}
