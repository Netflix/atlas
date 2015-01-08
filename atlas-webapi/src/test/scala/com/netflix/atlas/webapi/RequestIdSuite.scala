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

import org.scalatest.FunSuite

class RequestIdSuite extends FunSuite {

  test("next") {
    val id1 = RequestId.next
    val id2 = RequestId.next
    assert(id1 !== id2)
  }

  test("next with parent") {
    val id1 = RequestId.next
    val id2 = RequestId.next(id1)
    assert(id1 !== id2)
    assert(id1 === id2.parent.get)
  }

  test("apply") {
    val id1 = RequestId.next
    val id2 = RequestId(id1.toString)
    assert(id1 === id2)
  }

  test("apply with parent") {
    val id1 = RequestId.next
    val id2 = RequestId.next(id1)
    val id3 = RequestId(id2.toString)
    assert(id2 === id3)
  }

  test("retry id") {
    val id1 = RequestId.next
    val id2 = id1.retryId
    assert(id1 !== id2)
    assert(id1.parent === id2.parent)
    assert(id1.name === id2.name)
    assert(id1.attempt < id2.attempt)
  }
}

