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
package com.netflix.atlas.webapi

import com.netflix.atlas.core.model.Datapoint
import org.scalatest.FunSuite


class PublishApiJsonSuite extends FunSuite {

  test("encode and decode datapoint") {
    val original = Datapoint(Map("name" -> "foo", "id" -> "bar"), 42L, 1024.0)
    val decoded = PublishApi.decodeDatapoint(PublishApi.encodeDatapoint(original))
    assert(original === decoded)
  }

  test("encode and decode batch") {
    val commonTags = Map("id" -> "bar")
    val original = List(Datapoint(Map("name" -> "foo"), 42L, 1024.0))
    val decoded = PublishApi.decodeBatch(PublishApi.encodeBatch(commonTags, original))
    assert(original.map(d => d.copy(tags = d.tags ++ commonTags)) === decoded)
  }

  test("decode batch empty") {
    val decoded = PublishApi.decodeBatch("{}")
    assert(decoded.size === 0)
  }

  test("decode with legacy array value") {
    val expected = Datapoint(Map("name" -> "foo"), 42L, 1024.0)
    val decoded = PublishApi.decodeDatapoint(
      """{"tags":{"name":"foo"},"timestamp":42,"values":[1024.0]}""")
    assert(expected === decoded)
  }

  test("decode legacy batch empty") {
    val decoded = PublishApi.decodeBatch("""
      {
        "tags": {},
        "metrics": []
      }
      """)
    assert(decoded.size === 0)
  }

  test("decode legacy batch no tags") {
    val decoded = PublishApi.decodeBatch("""
      {
        "metrics": []
      }
      """)
    assert(decoded.size === 0)
  }

  test("decode legacy batch with tags before") {
    val decoded = PublishApi.decodeBatch("""
      {
        "tags": {
          "foo": "bar"
        },
        "metrics": [
          {
            "tags": {"name": "test"},
            "start": 123456789,
            "values": [1.0]
          }
        ]
      }
      """)
    assert(decoded.size === 1)
    assert(decoded.head.tags === Map("name" -> "test", "foo" -> "bar"))
  }

  test("decode legacy batch with tags after") {
    val decoded = PublishApi.decodeBatch("""
      {
        "metrics": [
          {
            "tags": {"name": "test"},
            "start": 123456789,
            "values": [1.0]
          }
        ],
        "tags": {
          "foo": "bar"
        }
      }
      """)
    assert(decoded.size === 1)
    assert(decoded.head.tags === Map("name" -> "test", "foo" -> "bar"))
  }

  test("decode legacy batch no tags metric") {
    val decoded = PublishApi.decodeBatch("""
      {
        "metrics": [
          {
            "tags": {"name": "test"},
            "start": 123456789,
            "values": [1.0]
          }
        ]
      }
      """)
    assert(decoded.size === 1)
  }

  test("decode list empty") {
    val decoded = PublishApi.decodeList("""
      []
      """)
    assert(decoded.size === 0)
  }

  test("decode list") {
    val decoded = PublishApi.decodeList("""
      [
        {
          "tags": {"name": "test"},
          "timestamp": 123456789,
          "values": 1.0
        }
      ]
      """)
    assert(decoded.size === 1)
  }
}
