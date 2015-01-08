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

import java.io.PrintStream
import java.net.URI

import akka.actor.Props
import com.netflix.atlas.core.db.StaticDatabase
import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.PngImage
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.test.GraphAssertions
import org.scalatest.FunSuite
import spray.http.MediaTypes._
import spray.http.HttpEntity
import spray.http.StatusCodes
import spray.testkit.ScalatestRouteTest


class GraphApiSuite extends FunSuite with ScalatestRouteTest {

  import scala.concurrent.duration._

  implicit val routeTestTimeout = RouteTestTimeout(5.second)

  val db = StaticDatabase.demo
  system.actorOf(Props(new LocalDatabaseActor(db)), "db")

  val endpoint = new GraphApi

  val template = Streams.scope(Streams.resource("examples.md")) { in => Streams.lines(in).toList }

  private val dataDir   = s"graph/data"

  private val baseDir = "atlas-webapi"
  private val goldenDir = s"$baseDir/src/test/resources/${getClass.getSimpleName}"
  private val targetDir = s"$baseDir/target/${getClass.getSimpleName}"
  private val graphAssertions = new GraphAssertions(goldenDir, targetDir)

  val bless = false

  override def afterAll() {
    graphAssertions.generateReport(getClass)
    genMarkdown
  }

  test("simple line") {
    Get("/api/v1/graph?q=1") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.OK)
    }
  }

  // Run examples found in template. This serves two purposes:
  // 1. Verify the api is generating the right images for these examples.
  // 2. Generate the images and create an updated markdown output that we can save to the wiki.
  template.filter(_.startsWith("/api/v1/graph")).foreach { uri =>
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

  def genMarkdown(): Unit = {
    val baseUri = "http://netflix.github.io/atlas/images/wiki/examples"
    Streams.scope(Streams.fileOut(s"$targetDir/examples.md")) { out =>
      val ps = new PrintStream(out)
      template.foreach { line =>
        if (line.startsWith("/api/v1/graph")) {
          val name = imageFileName(line)
          ps.println(formatQuery(line.trim))
          ps.println(s"![$name]($baseUri/$name)")
        } else {
          ps.println(line)
        }
      }
    }
  }

  private def imageFileName(uri: String): String = {
    s"${"%040x".format(Hash.sha1(uri)).substring(0, 8)}.png"
  }

  def formatQuery(line: String): String = {
    val uri = URI.create(line)
    val params = Strings.parseQueryString(uri.getQuery)
    val pstr = params.toList.sortWith(_._1 < _._1).flatMap { case (k, vs) =>
      vs.map { v => if (k == "q") formatQueryExpr(v) else s"$k=$v" }
    }
    s"```\n${uri.getPath}?\n  ${pstr.mkString("\n  &")}\n```\n"
  }

  def formatQueryExpr(q: String): String = {
    val parts = q.split(",").toList
    val buf = new StringBuilder
    buf.append("q=\n    ")
    parts.foreach { p =>
      if (p.startsWith(":"))
        buf.append(p).append(',').append("\n    ")
      else
        buf.append(p).append(',')
    }
    val s = buf.toString
    s.substring(0, s.lastIndexOf(","))
  }

}
