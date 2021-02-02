/*
 * Copyright 2014-2021 Netflix, Inc.
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
package com.netflix.atlas.core.rollup

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class RollupManagerSuite extends AnyFunSuite {

  test("cardinality stats") {
    val ts = Seq(
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node1"),
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node2"),
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node3"),
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node4", "cluster" -> "c2"),
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node5", "cluster" -> "c1"),
      //
      Map("nf.app" -> "app1", "name" -> "name1", "cluster" -> "c1"),
      Map("nf.app" -> "app1", "name" -> "name1", "cluster" -> "c2"),
      //
      Map("nf.app" -> "app1", "name" -> "name2", "node" -> "node1"),
      Map("nf.app" -> "app1", "name" -> "name2", "node" -> "node2"),
      Map("nf.app" -> "app1", "name" -> "name2", "node" -> "node3"),
      Map("nf.app" -> "app1", "name" -> "name2", "node" -> "node4"),
      Map("nf.app" -> "app1", "name" -> "name2", "node" -> "node5"),
      //
      Map("nf.app" -> "app2", "name" -> "name1", "node" -> "node1"),
      Map("nf.app" -> "app2", "name" -> "name1", "node" -> "node1"),
      Map("nf.app" -> "app2", "name" -> "name1", "node" -> "node2"),
      Map("nf.app" -> "app2", "name" -> "name1", "node" -> "node2"),
      Map("nf.app" -> "app2", "name" -> "name1", "node" -> "node3")
    )

    val config = ConfigFactory.parseString(
      """
        |atlas.auto-rollup = {
        |  prefix = [
        |    {
        |      key = "nf.app",
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

    val rollupManager = RollupManager.newInstance(config)
    ts.foreach(rollupManager.update)

    val app1 = rollupManager.cardinality(List("app1"))
    assert(app1 >= 10 && app1 <= 14) // actual is 12

    val app1Name2 = rollupManager.cardinality(List("app1", "name2"))
    assert(app1Name2 >= 4 && app1Name2 <= 6) // actual is 5

    val app2Name1Tag = rollupManager.cardinality(List("app2", "name1"), "node")
    assert(app2Name1Tag >= 2 && app2Name1Tag <= 4) // actual is 3
  }

  test("rollup") {
    val config = ConfigFactory.parseString(
      """
        |atlas.auto-rollup = {
        |  prefix = [
        |    {
        |      key = "nf.app",
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

    val rollupManager = RollupManager.newInstance(config)

    val ts = Array(
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node0"),
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node1"),
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node2"),
    )
    for (i <- ts.indices) {
      val result = rollupManager.update(ts(i))

      if (i < 2) {
        assert(result eq ts(i), "should be same tags object without hitting limit")
      }

      if (i == 2) {
        assert(
          result.getOrElse("node", "*") == RollupManager.RollupValue,
          "should rollup node after hitting tag limit"
        )
      }

    }
  }

  test("drop") {
    val config = ConfigFactory.parseString(
      """
        |atlas.auto-rollup = {
        |  prefix = [
        |    {
        |      key = "nf.app",
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

    val rollupManager = RollupManager.newInstance(config)

    val ts = Array(
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node1"),
      Map("nf.app" -> "app1", "name" -> "name2", "node" -> "node2"),
      Map("nf.app" -> "app1", "name" -> "name3", "node" -> "node3"),
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node4"),
      Map("nf.app" -> "app1", "name" -> "name4", "node" -> "node5"),
    )

    for (i <- ts.indices) {
      val result = rollupManager.update(ts(i))
      if (i < 2) {
        assert(result eq ts(i), "should be same tags object - no limit hit")
      }

      if (i == 2) {
        assert(result == null, "should drop -  number of names reaches 2")
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
