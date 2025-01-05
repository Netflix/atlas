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
package com.netflix.atlas.pekko

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import munit.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ClusterOpsSuite extends FunSuite {

  private implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)

  test("groupBy: empty cluster") {
    val input = List(
      ClusterOps.Cluster(Set.empty[String]),
      ClusterOps.Data(Map.empty[String, String])
    )
    val context = ClusterOps.GroupByContext[String, String, String](
      client = (_: String) => Flow[String].map(v => v)
    )
    val future = Source(input)
      .via(ClusterOps.groupBy(context))
      .runWith(Sink.seq[String])
    val seq = Await.result(future, Duration.Inf)
    assert(seq.isEmpty)
  }

  test("groupBy: data for nonexistent member") {
    val input = List(
      ClusterOps.Data(Map("a" -> "1"))
    )
    val context = ClusterOps.GroupByContext[String, String, String](
      client = (_: String) => Flow[String].map(v => v)
    )
    val future = Source(input)
      .via(ClusterOps.groupBy(context))
      .runWith(Sink.seq[String])
    val seq = Await.result(future, Duration.Inf)
    assert(seq.isEmpty)
  }

  test("groupBy: single member") {
    val input = List(
      ClusterOps.Cluster(Set("a")),
      ClusterOps.Data(Map("a" -> 1)),
      ClusterOps.Data(Map("a" -> 2)),
      ClusterOps.Data(Map("a" -> 3))
    )
    val context = ClusterOps.GroupByContext[String, Int, Int](
      client = (_: String) => Flow[Int].map(v => v),
      queueSize = 10
    )
    val future = Source(input)
      .via(ClusterOps.groupBy(context))
      .runWith(Sink.seq[Int])
    val seq = Await.result(future, Duration.Inf)
    assertEquals(seq, Seq(1, 2, 3))
  }

  test("groupBy: add and remove member") {
    val input = List(
      ClusterOps.Cluster(Set("a")),
      ClusterOps.Data(Map("a" -> 1)),
      ClusterOps.Data(Map("a" -> 2)),
      ClusterOps.Data(Map("a" -> 3)),
      ClusterOps.Cluster(Set.empty[String]),
      ClusterOps.Data(Map("a" -> 4)),
      ClusterOps.Data(Map("a" -> 5)),
      ClusterOps.Data(Map("a" -> 6)),
      ClusterOps.Cluster(Set("a")),
      ClusterOps.Data(Map("a" -> 7)),
      ClusterOps.Data(Map("a" -> 8)),
      ClusterOps.Data(Map("a" -> 9))
    )
    val context = ClusterOps.GroupByContext[String, Int, Int](
      client = (_: String) => Flow[Int].map(v => v),
      queueSize = 10
    )
    val future = Source(input)
      .via(ClusterOps.groupBy(context))
      .runWith(Sink.seq[Int])
    val seq = Await.result(future, Duration.Inf)
    assertEquals(seq.sortWith(_ < _), Seq(1, 2, 3, 7, 8, 9))
  }

  test("groupBy: multiple members") {
    val input = List(
      ClusterOps.Cluster(Set("a", "b")),
      ClusterOps.Data(Map("a" -> 1, "b" -> 2)),
      ClusterOps.Data(Map("a" -> 3, "b" -> 4)),
      ClusterOps.Data(Map("a" -> 5, "b" -> 6))
    )
    val context = ClusterOps.GroupByContext[String, Int, (String, Int)](
      client = (k: String) => Flow[Int].map(v => k -> v),
      queueSize = 10
    )
    val future = Source(input)
      .via(ClusterOps.groupBy(context))
      .runWith(Sink.seq[(String, Int)])
    val seq = Await.result(future, Duration.Inf)
    assertEquals(seq.filter(_._1 == "a").map(_._2), Seq(1, 3, 5))
    assertEquals(seq.filter(_._1 == "b").map(_._2), Seq(2, 4, 6))
  }

  test("groupBy: failed substream") {
    val input = List(
      ClusterOps.Cluster(Set("a", "b")),
      ClusterOps.Data(Map("a" -> 1, "b" -> 2)),
      ClusterOps.Data(Map("a" -> 3, "b" -> 4)),
      ClusterOps.Data(Map("a" -> 5, "b" -> 6))
    )
    val context = ClusterOps.GroupByContext[String, Int, (String, Int)](
      client = (k: String) =>
        Flow[Int].map { v =>
          if (v == 3) throw new RuntimeException("test")
          k -> v
        },
      queueSize = 10
    )
    val future = Source(input)
      .via(ClusterOps.groupBy(context))
      .runWith(Sink.seq[(String, Int)])
    val seq = Await.result(future, Duration.Inf)
    assertEquals(seq.filter(_._1 == "a").map(_._2), Seq(1, 5))
    assertEquals(seq.filter(_._1 == "b").map(_._2), Seq(2, 4, 6))
  }
}
