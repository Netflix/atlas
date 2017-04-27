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
package com.netflix.atlas.webapi

import akka.actor.Props
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.netflix.atlas.akka.RequestHandler
import com.netflix.atlas.core.db.StaticDatabase
import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.PngImage
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.test.GraphAssertions
import com.netflix.atlas.test.SrcPath
import org.scalatest.FunSuite


class GraphApiSuite extends FunSuite with ScalatestRouteTest {

  import scala.concurrent.duration._

  // Set to high value to avoid spurious failures with code coverage. Typically 5s shows no
  // issues outside of running with code coverage.
  implicit val routeTestTimeout = RouteTestTimeout(600.seconds)

  private val db = StaticDatabase.demo
  system.actorOf(Props(new LocalDatabaseActor(db)), "db")

  private val routes = RequestHandler.standardOptions((new GraphApi).routes)

  private val template = Streams.scope(Streams.resource("examples.md")) { in => Streams.lines(in).toList }
  private val others = Streams.scope(Streams.resource("others.md")) { in => Streams.lines(in).toList }
  private val all = template ::: others

  // SBT working directory gets updated with fork to be the dir for the project
  private val baseDir = SrcPath.forProject("atlas-webapi")
  private val goldenDir = s"$baseDir/src/test/resources/${getClass.getSimpleName}"
  private val targetDir = s"$baseDir/target/${getClass.getSimpleName}"
  private val graphAssertions = new GraphAssertions(goldenDir, targetDir)

  private val bless = false

  override def afterAll(): Unit = {
    graphAssertions.generateReport(getClass)
  }

  test("simple line") {
    Get("/api/v1/graph?q=1") ~> routes ~> check {
      assert(response.status === StatusCodes.OK)
    }
  }

  // Run examples found in template to verify images are generated correctly. To see diffs view
  // the report: $ open ./atlas-webapi/target/GraphApiSuite/report.html
  all.filter(_.startsWith("/api/v1/graph")).foreach { uri =>
    test(uri) {
      Get(uri) ~> routes ~> check {
        // Note: will fail prior to 8u20:
        // https://github.com/Netflix/atlas/issues/9
        assert(response.status === StatusCodes.OK)
        assert(response.entity.contentType.mediaType === MediaTypes.`image/png`)
        val image = PngImage(responseAs[Array[Byte]])
        graphAssertions.assertEquals(image, imageFileName(uri), bless)
      }
    }
  }

  private def imageFileName(uri: String): String = {
    s"${Strings.zeroPad(Hash.sha1(uri), 40).substring(0, 8)}.png"
  }

}
