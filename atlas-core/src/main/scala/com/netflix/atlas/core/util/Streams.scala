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
package com.netflix.atlas.core.util

import java.io.*
import java.nio.file.Files
import java.nio.file.Paths
import java.util.zip.*

import scala.util.Using

object Streams {

  def resource(name: String): InputStream = {
    val url = getClass.getClassLoader.getResource(name)
    if (url == null)
      throw new FileNotFoundException(s"resource: $name")
    url.openStream()
  }

  def fileIn(name: String): InputStream = Files.newInputStream(Paths.get(name))

  def fileIn(file: File): InputStream = Files.newInputStream(file.toPath)

  def fileOut(name: String): OutputStream = Files.newOutputStream(Paths.get(name))

  def fileOut(file: File): OutputStream = Files.newOutputStream(file.toPath)

  def byteArray(f: OutputStream => Unit): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    f(baos)
    baos.toByteArray
  }

  def byteArray(input: InputStream): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val buf = new Array[Byte](1024)
    var len = input.read(buf)
    while (len > 0) {
      baos.write(buf, 0, len)
      len = input.read(buf)
    }
    baos.toByteArray
  }

  def string(f: Writer => Unit): String = {
    val w = new StringWriter
    f(w)
    w.toString
  }

  def gzip(output: OutputStream): OutputStream = {
    new GZIPOutputStream(output)
  }

  def gzip(input: InputStream): InputStream = {
    new GZIPInputStream(input)
  }

  def reader(input: InputStream): BufferedReader = {
    new BufferedReader(new InputStreamReader(input, "UTF-8"))
  }

  def lines(input: InputStream): Iterator[String] = {
    lines(reader(input))
  }

  def lines(input: Reader): Iterator[String] = {
    val reader = input match {
      case r: BufferedReader => r
      case r: Reader         => new BufferedReader(r)
    }

    new Iterator[String] {
      private var value = reader.readLine()

      def hasNext: Boolean = value != null

      def next(): String = {
        val tmp = value
        value = reader.readLine()
        tmp
      }
    }
  }

  @deprecated("Use scala.util.Using instead.", "1.7.0")
  def scope[R <: AutoCloseable, T](res: R)(f: R => T): T = {
    Using.resource(res)(f)
  }
}
