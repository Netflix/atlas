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

import akka.actor.Props
import com.netflix.atlas.core.db.StaticDatabase
import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.PngImage
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.test.GraphAssertions
import org.scalatest.FunSuite
import spray.http.MediaTypes._
import spray.http.HttpEntity
import spray.http.StatusCodes
import spray.testkit.ScalatestRouteTest


class GraphApiSuite extends FunSuite with ScalatestRouteTest {

  import scala.concurrent.duration._

  // Set to high value to avoid spurious failures with code coverage. Typically 5s shows no
  // issues outside of running with code coverage.
  implicit val routeTestTimeout = RouteTestTimeout(600.seconds)

  val db = StaticDatabase.demo
  system.actorOf(Props(new LocalDatabaseActor(db)), "db")

  val endpoint = new GraphApi

  val template = Streams.scope(Streams.resource("examples.md")) { in => Streams.lines(in).toList }
  val others = Streams.scope(Streams.resource("others.md")) { in => Streams.lines(in).toList }
  val all = template ::: others

  private val dataDir   = s"graph/data"

  // SBT working directory gets updated with fork to be the dir for the project
  private val baseDir = "."
  private val goldenDir = s"$baseDir/src/test/resources/${getClass.getSimpleName}"
  private val targetDir = s"$baseDir/target/${getClass.getSimpleName}"
  private val graphAssertions = new GraphAssertions(goldenDir, targetDir)

  val bless = false

  override def afterAll() {
    graphAssertions.generateReport(getClass)
  }

  test("simple line") {
    Get("/api/v1/graph?q=1") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.OK)
    }
  }

  // Run examples found in template to verify images are generated correctly. To see diffs view
  // the report: $ open ./atlas-webapi/target/GraphApiSuite/report.html
  all.filter(_.startsWith("/api/v1/graph")).foreach { uri =>
    test(uri) {
      Get(uri) ~> endpoint.routes ~> check {
        // Note: will fail prior to 8u20:
        // https://github.com/Netflix/atlas/issues/9
        assert(response.status === StatusCodes.OK)
        response.entity match {
          case e: HttpEntity.NonEmpty => assert(e.contentType.mediaType === `image/png`)
          case _                      => fail("empty response")
        }
        val image = PngImage(response.entity.data.toByteArray)
        graphAssertions.assertEquals(image, imageFileName(uri), bless)
      }
    }
  }

  private def imageFileName(uri: String): String = {
    s"${"%040x".format(Hash.sha1(uri)).substring(0, 8)}.png"
  }

}
