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
package com.netflix.atlas.eval

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.eval.stream.EvaluationFlows
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Check performance of json deserialization of messages.
  *
  * ```
  * > jmh:run -prof jmh.extras.JFR -wi 10 -i 10 -f1 -t1 .*ServoMessageToDatapoint.*
  * ...
  * [info] Benchmark                       Mode  Cnt  Score   Error  Units
  * [info] servoMessagesToDatapoints      thrpt   10  7.677 ± 0.325  ops/s
  * ```
  */
@State(Scope.Thread)
class ServoMessageToDatapoint {

  private val servoMetrics = (0 until 100).toList.map { i =>
    val node = f"i-$i%017d"
    val config = s"""{"name":"jvm.gc.pause","tags":{"nf.cluster":"foo-test","nf.node":"$node"}}"""
    s"""{"config":$config,"timestamp":1492462554000,"value":$i.0}"""
  }
  private val servoMsg = s"""data: {"metrics":[${servoMetrics.mkString(",")}]}"""

  private val servoMsgs = (0 until 1000).map(_ => ByteString(servoMsg)).toList

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  @Threads(1)
  @Benchmark
  def servoMessagesToDatapoints(bh: Blackhole): Unit = {
    val future = Source(servoMsgs)
      .via(EvaluationFlows.servoMessagesToDatapoints(60000))
      .runWith(Sink.seq[Datapoint])
    val result = Await.result(future, Duration.Inf)
    bh.consume(result)
  }

  @TearDown
  def shutdown(): Unit = {
    materializer.shutdown()
    system.terminate()
  }

}
