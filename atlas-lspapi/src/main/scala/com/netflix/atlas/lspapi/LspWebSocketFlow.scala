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

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.ws.Message
import org.apache.pekko.http.scaladsl.model.ws.TextMessage
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.eclipse.lsp4j.jsonrpc.MessageConsumer
import org.eclipse.lsp4j.jsonrpc.RemoteEndpoint
import org.eclipse.lsp4j.jsonrpc.json.MessageJsonHandler
import org.eclipse.lsp4j.jsonrpc.services.ServiceEndpoints
import org.eclipse.lsp4j.services.LanguageClient
import org.eclipse.lsp4j.services.LanguageClientAware
import org.eclipse.lsp4j.services.LanguageServer

import scala.jdk.CollectionConverters.*

/**
  * Creates a Pekko Streams `Flow[Message, Message, NotUsed]` that bridges a WebSocket
  * connection to an LSP4j language server. Each WebSocket connection gets its own server
  * instance. JSON-RPC messages are exchanged directly without Content-Length framing.
  */
object LspWebSocketFlow {

  /** Maximum size of a single incoming message in characters. */
  private val MaxMessageSize = 1024 * 1024

  /**
    * Create a WebSocket flow for an LSP server.
    *
    * @param serverFactory
    *     Factory that creates a new server instance per connection.
    * @param materializer
    *     Materializer for running streams.
    */
  def apply(
    serverFactory: () => LanguageServer with LanguageClientAware
  )(implicit materializer: Materializer): Flow[Message, Message, NotUsed] = {

    val server = serverFactory()

    // Collect supported methods from both server and client sides
    val serverMethods = ServiceEndpoints.getSupportedMethods(server.getClass).asScala
    val clientMethods =
      ServiceEndpoints.getSupportedMethods(classOf[LanguageClient]).asScala
    val allMethods = (serverMethods ++ clientMethods).asJava

    val jsonHandler = new MessageJsonHandler(allMethods)

    // Outgoing: source queue that the server writes to
    val (queue, outgoing) = Source
      .queue[TextMessage](64, OverflowStrategy.fail)
      .preMaterialize()

    val outgoingConsumer: MessageConsumer = { msg =>
      val json = jsonHandler.serialize(msg)
      queue.offer(TextMessage.Strict(json))
    }

    // Wire up LSP4j endpoints
    val localEndpoint = ServiceEndpoints.toEndpoint(server)
    val remoteEndpoint = new RemoteEndpoint(outgoingConsumer, localEndpoint)
    jsonHandler.setMethodProvider(remoteEndpoint)

    // Create client proxy and connect to server
    val clientProxy = ServiceEndpoints.toServiceObject(remoteEndpoint, classOf[LanguageClient])
    server.connect(clientProxy)

    // Incoming: sink that feeds WebSocket messages to the server
    val incoming: Sink[Message, NotUsed] = Flow[Message]
      .flatMapConcat {
        case TextMessage.Strict(text) =>
          Source.single(text)
        case TextMessage.Streamed(textStream) =>
          textStream.fold("") { (acc, chunk) =>
            val combined = acc + chunk
            if (combined.length > MaxMessageSize)
              throw new IllegalStateException("LSP message too large")
            combined
          }
        case _ =>
          Source.single(null)
      }
      .filter(_ != null)
      .to(Sink.foreach { text =>
        val msg = jsonHandler.parseMessage(text)
        remoteEndpoint.consume(msg)
      })
      .mapMaterializedValue(_ => NotUsed)

    Flow
      .fromSinkAndSourceCoupled(incoming, outgoing)
      .watchTermination() { (_, done) =>
        done.foreach { _ =>
          server.shutdown()
          server.exit()
          queue.complete()
        }(materializer.executionContext)
        NotUsed
      }
  }
}
