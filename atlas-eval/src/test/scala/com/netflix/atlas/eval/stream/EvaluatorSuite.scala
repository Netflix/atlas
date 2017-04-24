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
package com.netflix.atlas.eval.stream

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Duration

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.test.SrcPath
import com.netflix.spectator.api.DefaultRegistry
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite

import scala.concurrent.Await

class EvaluatorSuite extends FunSuite with BeforeAndAfter {

  private val targetDir = Paths.get(SrcPath.forProject("atlas-eval"), "target", "EvaluatorSuite")
  private val resourcesDir = Paths.get(SrcPath.forProject("atlas-eval"), "src", "test", "resources")

  private val config = ConfigFactory.load()
  private val registry = new DefaultRegistry()
  private implicit val system = ActorSystem("test", config)
  private implicit val materializer = ActorMaterializer()

  before {
    Files.createDirectories(targetDir)
  }

  def testPublisher(baseUri: String): Unit = {
    import scala.concurrent.duration._

    val evaluator = new Evaluator(config, registry, system)

    val uri = s"$baseUri?q=name,jvm.gc.pause,:eq,:dist-max,(,nf.asg,nf.node,),:by"
    val future = Source.fromPublisher(evaluator.createPublisher(uri))
      .toMat(Sink.seq[TimeSeriesMessage])(Keep.right)
      .run()

    val messages = Await.result(future, 1.minute)
    assert(messages.size === 256)
    assert(messages.map(_.tags("nf.asg")).toSet.size === 3)
  }

  test("write to file") {
    val evaluator = new Evaluator(config, registry, system)

    val uri = "resource:///gc-pause.dat?q=name,jvm.gc.pause,:eq,:dist-max,(,nf.asg,nf.node,),:by"
    val path = targetDir.resolve("test.dat")
    val duration = Duration.ofMinutes(1)
    evaluator.writeInputToFile(uri, path, duration)

    val expected = Hash.sha1(Files.readAllBytes(resourcesDir.resolve("gc-pause.dat")))
    val actual = Hash.sha1(Files.readAllBytes(path))
    assert(expected === actual)
  }

  test("create publisher from file uri") {
    testPublisher(s"$resourcesDir/gc-pause.dat")
  }

  test("create publisher from resource uri") {
    testPublisher("resource:///gc-pause.dat")
  }
}
