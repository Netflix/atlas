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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.util.Hash

import java.math.BigInteger

import org.scalatest.FunSuite

class TaggedItemSuite extends FunSuite {

  def expectedId(tags: Map[String, String]): BigInteger = {
    Hash.sha1(tags.toList.sortWith(_._1 < _._1).map(t => t._1 + "=" + t._2).mkString(","))
  }

  test("computeId, name only") {
    val t1 = Map("name" -> "foo")
    val t2 = Map("name" -> "bar")
    assert(TaggedItem.computeId(t1) === expectedId(t1))
    assert(TaggedItem.computeId(t2) === expectedId(t2))
    assert(TaggedItem.computeId(t1) != TaggedItem.computeId(t2))
  }

  test("computeId, multi") {
    val t1 = Map("name" -> "foo", "cluster" -> "abc", "app" -> "a", "zone" -> "1")
    val t2 = Map("name" -> "foo", "cluster" -> "abc", "app" -> "a", "zone" -> "2")
    assert(TaggedItem.computeId(t1) === expectedId(t1))
    assert(TaggedItem.computeId(t2) === expectedId(t2))
    assert(TaggedItem.computeId(t1) != TaggedItem.computeId(t2))
  }

}
