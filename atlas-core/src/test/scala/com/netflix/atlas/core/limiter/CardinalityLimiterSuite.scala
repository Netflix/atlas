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
package com.netflix.atlas.core.limiter

import com.netflix.atlas.core.model.Query
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class CardinalityLimiterSuite extends FunSuite {

  test("cardinality stats") {
    val ts = Seq(
      Map("app" -> "app1", "name" -> "name1", "node" -> "node1"),
      Map("app" -> "app1", "name" -> "name1", "node" -> "node2"),
      Map("app" -> "app1", "name" -> "name1", "node" -> "node3"),
      Map("app" -> "app1", "name" -> "name1", "node" -> "node4", "cluster" -> "c2"),
      Map("app" -> "app1", "name" -> "name1", "node" -> "node5", "cluster" -> "c1"),
      //
      Map("app" -> "app1", "name" -> "name1", "cluster" -> "c1"),
      Map("app" -> "app1", "name" -> "name1", "cluster" -> "c2"),
      //
      Map("app" -> "app1", "name" -> "name2", "node" -> "node1"),
      Map("app" -> "app1", "name" -> "name2", "node" -> "node2"),
      Map("app" -> "app1", "name" -> "name2", "node" -> "node3"),
      Map("app" -> "app1", "name" -> "name2", "node" -> "node4"),
      Map("app" -> "app1", "name" -> "name2", "node" -> "node5"),
      //
      Map("app" -> "app2", "name" -> "name1", "node" -> "node1"),
      Map("app" -> "app2", "name" -> "name1", "node" -> "node1"),
      Map("app" -> "app2", "name" -> "name1", "node" -> "node2"),
      Map("app" -> "app2", "name" -> "name1", "node" -> "node2"),
      Map("app" -> "app2", "name" -> "name1", "node" -> "node3")
    )

    val config = ConfigFactory.parseString(
      """
        |atlas.auto-rollup = {
        |  prefix = [
        |    {
        |      key = "app",
        |      value-limit = 100,
        |      total-limit = 100,
        |    },
        |    {
        |      key = "name",
        |      value-limit = 100,
        |      total-limit = 100,
        |    }
        |  ],
        |  tag-value-limit = 100
        |}
    """.stripMargin
    )

    val limiter = CardinalityLimiter.newInstance(config)
    ts.foreach(limiter.update)

    val app1 = limiter.cardinalityBy(List("app1"))
    assert(app1 >= 10 && app1 <= 14) // actual is 12

    val app1Name2 = limiter.cardinalityBy(List("app1", "name2"))
    assert(app1Name2 >= 4 && app1Name2 <= 6) // actual is 5

    val app2Name1Tag = limiter.cardinalityBy(List("app2", "name1"), "node")
    assert(app2Name1Tag >= 2 && app2Name1Tag <= 4) // actual is 3

    val query1 = Query.Equal("app", "app1").and(Query.Equal("name", "name1"))
    assert(limiter.canServe(query1))

  }

  test("rollup") {
    val config = ConfigFactory.parseString(
      """
        |atlas.auto-rollup = {
        |  prefix = [
        |    {
        |      key = "app",
        |      value-limit = 100,
        |      total-limit = 100,
        |    },
        |    {
        |      key = "name",
        |      value-limit = 100,
        |      total-limit = 100,
        |    }
        |  ],
        |  tag-value-limit = 2
        |}
    """.stripMargin
    )

    val limiter = CardinalityLimiter.newInstance(config)

    val ts = Array(
      Map("app" -> "app1", "name" -> "name1", "node" -> "node0"),
      Map("app" -> "app1", "name" -> "name1", "node" -> "node1"),
      Map("app" -> "app1", "name" -> "name1", "node" -> "node2")
    )

    val queryAppName = Query.Equal("app", "app1").and(Query.Equal("name", "name1"))
    val queryAppNameNode = queryAppName.and(Query.Equal("node", "1"))
    for (i <- ts.indices) {
      val result = limiter.update(ts(i))

      if (i == 0) {
        assert(result eq ts(i), "should be same tags object without hitting limit")
        assert(limiter.canServe(queryAppName))
        assert(limiter.canServe(queryAppNameNode))
      }

      if (i == 1) {
        assert(result eq ts(i), "should be same tags object without hitting limit")
        assert(limiter.canServe(queryAppName))
        // reach tag limit 2
        assert(!limiter.canServe(queryAppNameNode))
      }

      if (i == 2) {
        assert(
          result.getOrElse("node", "*") == CardinalityLimiter.RollupValue,
          "should rollup node after hitting tag limit"
        )
        assert(limiter.canServe(queryAppName))
        // exceed tag limit 2
        assert(!limiter.canServe(queryAppNameNode))
      }

    }
  }

  test("drop") {
    val config = ConfigFactory.parseString(
      """
        |atlas.auto-rollup = {
        |  prefix = [
        |    {
        |      key = "app",
        |      value-limit = 100,
        |      total-limit = 0,
        |    },
        |    {
        |      key = "name",
        |      value-limit = 2,
        |      total-limit = 3,
        |    }
        |  ],
        |  tag-value-limit = 100
        |}
    """.stripMargin
    )

    val limiter = CardinalityLimiter.newInstance(config)

    val ts = Array(
      Map("app" -> "app1", "name" -> "name1", "node" -> "node1"),
      Map("app" -> "app1", "name" -> "name2", "node" -> "node2"),
      Map("app" -> "app1", "name" -> "name3", "node" -> "node3"),
      Map("app" -> "app1", "name" -> "name1", "node" -> "node4"),
      Map("app" -> "app1", "name" -> "name4", "node" -> "node5")
    )

    val queryName1 = Query.Equal("app", "app1").and(Query.Equal("name", "name1"))
    val queryName3 = Query.Equal("app", "app1").and(Query.Equal("name", "name3"))

    for (i <- ts.indices) {
      val result = limiter.update(ts(i))
      if (i < 2) {
        assert(result eq ts(i), "should be same tags object - no limit hit")
        assert(limiter.canServe(queryName1))
        assert(limiter.canServe(queryName3))
      }

      if (i == 2) {
        assert(result == null, "should drop -  number of names reaches 2")
        assert(limiter.canServe(queryName1))
        assert(!limiter.canServe(queryName3)) // name3 dropped
      }

      if (i == 3) {
        assert(result eq ts(i), "should be same tags object - no limit hit")
      }

      if (i == 4) {
        assert(result == null, "should drop -  app1 ts reaches 3 and sees new name")
      }
    }
  }
}
