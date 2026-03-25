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
package com.netflix.atlas.lspapi

import com.netflix.atlas.pekko.RequestHandler
import com.netflix.atlas.pekko.testkit.MUnitRouteSuite
import com.typesafe.config.ConfigFactory
import org.apache.pekko.http.scaladsl.testkit.WSProbe

class LspApiSuite extends MUnitRouteSuite {

  private val config = ConfigFactory.load()
  private val api = new LspApi(config)
  private val routes = RequestHandler.standardOptions(api.routes)

  private def initializeRequest(id: Int): String = {
    s"""{"jsonrpc":"2.0","id":$id,"method":"initialize","params":{"capabilities":{}}}"""
  }

  test("asl websocket upgrade") {
    val client = WSProbe()
    WS("/lsp/metrics/asl", client.flow) ~> routes ~> check {
      assert(isWebSocketUpgrade)
      client.sendMessage(initializeRequest(1))
      val response = client.expectMessage()
      val text = response.asTextMessage.getStrictText
      assert(text.contains("\"id\":1"))
      assert(text.contains("\"result\""))
      client.sendCompletion()
    }
  }

  test("uri websocket upgrade") {
    val client = WSProbe()
    WS("/lsp/metrics/uri", client.flow) ~> routes ~> check {
      assert(isWebSocketUpgrade)
      client.sendMessage(initializeRequest(1))
      val response = client.expectMessage()
      val text = response.asTextMessage.getStrictText
      assert(text.contains("\"id\":1"))
      assert(text.contains("\"result\""))
      client.sendCompletion()
    }
  }
}
