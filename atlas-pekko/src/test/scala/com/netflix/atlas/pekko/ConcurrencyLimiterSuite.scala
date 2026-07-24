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

import munit.FunSuite

class ConcurrencyLimiterSuite extends FunSuite {

  test("acquire and release a single permit") {
    val limiter = ConcurrencyLimiter(2)
    assert(limiter.tryAcquire("a", 1))
    assertEquals(limiter.usedPermits, 1)
    limiter.release("a", 1)
    assertEquals(limiter.usedPermits, 0)
  }

  test("weighted acquire consumes multiple permits") {
    val limiter = ConcurrencyLimiter(10)
    assert(limiter.tryAcquire("a", 4))
    assertEquals(limiter.usedPermits, 4)
    assert(limiter.tryAcquire("b", 6))
    assertEquals(limiter.usedPermits, 10)
    assert(!limiter.tryAcquire("c", 1))
  }

  test("acquire fails when insufficient permits remain") {
    val limiter = ConcurrencyLimiter(5)
    assert(limiter.tryAcquire("a", 3))
    assert(!limiter.tryAcquire("b", 3))
    assertEquals(limiter.usedPermits, 3)
    limiter.release("a", 3)
    assert(limiter.tryAcquire("b", 3))
  }

  test("cost is clamped to the budget so a large request can still be admitted when idle") {
    val limiter = ConcurrencyLimiter(4)
    assert(limiter.tryAcquire("a", 100))
    assertEquals(limiter.usedPermits, 4)
    // A second request cannot get in until the first releases.
    assert(!limiter.tryAcquire("b", 1))
    limiter.release("a", 100)
    assertEquals(limiter.usedPermits, 0)
  }

  test("cost below one is treated as one") {
    val limiter = ConcurrencyLimiter(2)
    assert(limiter.tryAcquire("a", 0))
    assertEquals(limiter.usedPermits, 1)
    assert(limiter.tryAcquire("b", -5))
    assertEquals(limiter.usedPermits, 2)
  }

  test("budget of zero or less produces a limiter that always denies") {
    assertEquals(ConcurrencyLimiter(0), NoneLimiter)
    assertEquals(ConcurrencyLimiter(-1), NoneLimiter)
    assert(!NoneLimiter.tryAcquire("a", 1))
    assertEquals(NoneLimiter.maxPermits, 0)
  }

  test("unlimited limiter always permits") {
    val limiter = ConcurrencyLimiter.Unlimited
    assert(limiter.tryAcquire("a", Int.MaxValue))
    assertEquals(limiter.usedPermits, 0)
    assertEquals(limiter.maxPermits, Int.MaxValue)
    limiter.release("a", Int.MaxValue)
  }

  test("maxPermits reflects the configured budget") {
    assertEquals(ConcurrencyLimiter(7).maxPermits, 7)
  }
}
