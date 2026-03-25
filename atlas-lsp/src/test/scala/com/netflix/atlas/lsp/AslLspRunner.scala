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
package com.netflix.atlas.lsp

import com.netflix.atlas.core.model.CustomVocabulary

import java.io.OutputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import com.typesafe.config.ConfigFactory
import org.eclipse.lsp4j.launch.LSPLauncher
import org.java_websocket.WebSocket
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.server.WebSocketServer

/**
  * Test server for experimenting with Atlas LSP features in a browser.
  *
  * Runs a WebSocket server on port 7102 that speaks standard LSP JSON-RPC.
  * Use with the test client in atlas-lsp/test-client/:
  *
  *   1. Start this server:
  *      project/sbt 'atlas-lsp/test:runMain com.netflix.atlas.lsp.AslLspRunner'
  *
  *   2. In another terminal, start the test client:
  *      cd atlas-lsp/test-client && npm install && npm run dev
  *
  *   3. Open the URL printed by Vite (typically http://localhost:5173)
  */
object AslLspRunner {

  def main(args: Array[String]): Unit = {
    val wsPort = 7102

    // WebSocket server for LSP JSON-RPC
    val wsServer = new LspWebSocketServer(new InetSocketAddress(wsPort))
    wsServer.start()

    println(s"Atlas LSP WebSocket:   ws://localhost:$wsPort")
    println()
    println("Start the test client in another terminal:")
    println("  cd atlas-lsp/test-client && npm install && npm run dev")
    println()
    println("Press Ctrl+C to stop")

    // Block so the JVM doesn't exit (WebSocket threads are daemon threads)
    Thread.currentThread().join()
  }

  /**
    * WebSocket server that bridges each connection to an LSP4j Launcher.
    * Each WebSocket message is a raw JSON-RPC payload (no Content-Length framing).
    * LSP4j expects Content-Length framing on its InputStream, so we wrap messages
    * before piping them in, and strip framing from the OutputStream before sending.
    */
  private class LspWebSocketServer(address: InetSocketAddress) extends WebSocketServer(address) {

    setReuseAddr(true)

    // Per-connection state: the piped stream feeding LSP4j
    private val connections =
      new java.util.concurrent.ConcurrentHashMap[WebSocket, PipedOutputStream]

    override def onOpen(conn: WebSocket, handshake: ClientHandshake): Unit = {
      val path = handshake.getResourceDescriptor
      println(s"LSP client connected: ${conn.getRemoteSocketAddress} (path: $path)")

      // Pipe: we write Content-Length framed messages into pipedOut,
      // LSP4j reads from pipedIn
      val pipedOut = new PipedOutputStream
      val pipedIn = new PipedInputStream(pipedOut, 65536)
      connections.put(conn, pipedOut)

      // Output stream that captures LSP4j responses and sends them over WebSocket
      // LSP4j writes Content-Length framed messages; we strip the framing
      val wsOut = new WebSocketOutputStream(conn)

      val glossary = Glossary.load("sample-glossary.json")
      val vocabulary = new CustomVocabulary(ConfigFactory.load())
      val server =
        if (path == "/uri") new UriLspServer(vocabulary, glossary)
        else new AslLspServer(vocabulary, glossary)
      val launcher = LSPLauncher.createServerLauncher(server, pipedIn, wsOut)
      server.connect(launcher.getRemoteProxy)

      // Start listening on a daemon thread
      val runnable: Runnable = () => {
        try {
          launcher.startListening().get()
        } catch {
          case _: Exception => // connection closed
        }
      }
      val thread = new Thread(runnable)
      thread.setDaemon(true)
      thread.start()
    }

    override def onMessage(conn: WebSocket, message: String): Unit = {
      println(s"LSP <<< $message")
      val pipedOut = connections.get(conn)
      if (pipedOut != null) {
        // Wrap as Content-Length framed message for LSP4j
        val content = message.getBytes(StandardCharsets.UTF_8)
        val header = s"Content-Length: ${content.length}\r\n\r\n"
        try {
          pipedOut.synchronized {
            pipedOut.write(header.getBytes(StandardCharsets.US_ASCII))
            pipedOut.write(content)
            pipedOut.flush()
          }
        } catch {
          case _: Exception => conn.close()
        }
      }
    }

    override def onClose(conn: WebSocket, code: Int, reason: String, remote: Boolean): Unit = {
      println(s"LSP client disconnected: $reason")
      val pipedOut = connections.remove(conn)
      if (pipedOut != null) {
        try pipedOut.close()
        catch { case _: Exception => }
      }
    }

    override def onError(conn: WebSocket, ex: Exception): Unit = {
      println(s"LSP WebSocket error: ${ex.getMessage}")
    }

    override def onStart(): Unit = {}
  }

  /**
    * OutputStream that intercepts LSP4j's Content-Length framed output,
    * strips the headers, and sends raw JSON over the WebSocket.
    */
  private class WebSocketOutputStream(conn: WebSocket) extends OutputStream {

    private val buffer = new java.io.ByteArrayOutputStream(4096)

    override def write(b: Int): Unit = {
      buffer.synchronized {
        buffer.write(b)
        tryFlush()
      }
    }

    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      buffer.synchronized {
        buffer.write(b, off, len)
        tryFlush()
      }
    }

    private def tryFlush(): Unit = {
      val data = buffer.toByteArray
      val str = new String(data, StandardCharsets.US_ASCII)
      // Look for the end of the Content-Length header block
      val headerEnd = str.indexOf("\r\n\r\n")
      if (headerEnd < 0) return

      // Parse Content-Length
      val headerStr = str.substring(0, headerEnd)
      val clPrefix = "Content-Length: "
      val clLine = headerStr.split("\r\n").find(_.startsWith(clPrefix))
      if (clLine.isEmpty) return
      val contentLength = clLine.get.substring(clPrefix.length).trim.toInt

      val bodyStart = headerEnd + 4
      if (data.length < bodyStart + contentLength) return // not enough data yet

      val body = new String(data, bodyStart, contentLength, StandardCharsets.UTF_8)
      println(s"LSP >>> $body")
      if (conn.isOpen) {
        conn.send(body)
      }

      // Keep any remaining data after this message
      val consumed = bodyStart + contentLength
      buffer.reset()
      if (consumed < data.length) {
        buffer.write(data, consumed, data.length - consumed)
      }
    }
  }
}
