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
package com.netflix.atlas.lwcapi

import org.scalatest.FunSuite

class TTLManagerSuite extends FunSuite {
  test("touch and lastTouched") {
    val t = new TTLManager[String]()

    assert(t.lastTouched("this") === 0)

    t.touch("this", 123)
    assert(t.lastTouched("this") === 123)

    t.touch("that", 124)

    // At this point, we have "this" at 123 and "that" at 124.

    // Nothing occurs with a value smaller than 122.
    assert(t.needsTouch(122) === None)

    // at time 123, "this" matches, and is removed.
    assert(t.needsTouch(123) === Some("this"))
    assert(t.lastTouched("this") === 0)

    // At time >= 124, "that" is found and removed.
    assert(t.needsTouch(130) === Some("that"))

    // Nothing is left.
    assert(t.needsTouch(99999) === None)
  }

  test("remove") {
    val t = new TTLManager[String]()

    t.touch("this", 100)
    t.touch("that", 200)
    t.touch("those", 300)

    t.remove("that")

    assert(t.needsTouch(9999) === Some("this"))
    assert(t.needsTouch(9999) === Some("those"))
  }

  test("retouch") {
    val t = new TTLManager[String]()

    t.touch("this", 100)
    t.touch("that", 200)
    t.touch("this", 300)

    assert(t.needsTouch(50) === None)
    assert(t.needsTouch(250) === Some("that"))
    assert(t.needsTouch(350) === Some("this"))
  }

}
