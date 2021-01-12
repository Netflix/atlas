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

import org.scalatest.funsuite.AnyFunSuite

class CardinalityStatsSuite extends AnyFunSuite {
  test("cardinality by app,name,tags") {
    val ts = Seq(
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node1"),
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node2"),
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node3"),
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node4"),
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node5"),
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node6"),
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node7"),
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node8"),
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node9"),
      Map("nf.app" -> "app1", "name" -> "name1", "node" -> "node10"),
      Map("nf.app" -> "app1", "name" -> "name2", "node" -> "node1"),
      Map("nf.app" -> "app1", "name" -> "name2", "node" -> "node2"),
      Map("nf.app" -> "app1", "name" -> "name2", "node" -> "node3"),
      Map("nf.app" -> "app1", "name" -> "name2", "node" -> "node4"),
      Map("nf.app" -> "app1", "name" -> "name2", "node" -> "node5"),
      Map("nf.app" -> "app2", "name" -> "name1", "node" -> "node1"),
      Map("nf.app" -> "app2", "name" -> "name1", "node" -> "node1"),
      Map("nf.app" -> "app2", "name" -> "name1", "node" -> "node1"),
      Map("nf.app" -> "app2", "name" -> "name1", "node" -> "node1"),
      Map("nf.app" -> "app2", "name" -> "name1", "node" -> "node1")
    )

    val stats = new CardinalityStats
    ts.foreach(stats.update)

    // get methods for cardinality
    val app1 = stats.appCardinality("app1")
    assert(app1 >= 13 && app1 <= 17) // 15
    val app1Name1 = stats.appNameCardinality("app1", "name1")
    assert(app1Name1 >= 8 && app1Name1 <= 12) // 10
    val app1Name2Node = stats.appNameTagCardinality("app1", "name2", "node")
    assert(app1Name2Node >= 4 && app1Name2Node <= 6) // 5
    val app2Name1Node = stats.appNameTagCardinality("app2", "name1", "node")
    assert(app2Name1Node <= 2) // 1 - repeating time series should not increase count

    // Not existing ones should be 0
    assert(stats.appCardinality("appx") == 0)
    assert(stats.appNameCardinality("app1", "namex") == 0)
    assert(stats.appNameTagCardinality("app1", "name1", "nodex") == 0)

    // App Cardinality Iterator
    assert(
      stats.appCardinalityIterator().toList.sorted ==
        List(("app1", stats.appCardinality("app1")), ("app2", stats.appCardinality("app2")))
    )

    //App-Name Cardinality Iterator
    assert(
      stats.appNameCardinalityIterator().toList.sorted ==
        List(
          ("app1", "name1", stats.appNameCardinality("app1", "name1")),
          ("app1", "name2", stats.appNameCardinality("app1", "name2")),
          ("app2", "name1", stats.appNameCardinality("app2", "name1"))
        )
    )

    //App-Name-Tag Cardinality Iterator
    assert(
      stats.appNameTagCardinalityIterator().toList.sorted ==
        List(
          ("app1", "name1", "node", stats.appNameTagCardinality("app1", "name1", "node")),
          ("app1", "name2", "node", stats.appNameTagCardinality("app1", "name2", "node")),
          ("app2", "name1", "node", stats.appNameTagCardinality("app2", "name1", "node"))
        )
    )
  }
}
