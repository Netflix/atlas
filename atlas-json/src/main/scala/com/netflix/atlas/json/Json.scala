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
package com.netflix.atlas.json

import java.io.InputStream
import java.io.OutputStream
import java.io.Reader
import java.io.Writer
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonGenerator.Feature._
import com.fasterxml.jackson.core.JsonParser.Feature._
import com.fasterxml.jackson.core._
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule


object Json {

  final class Decoder[T: Manifest](reader: ObjectReader, factory: JsonFactory) {

    def decode(json: Array[Byte]): T = decode(factory.createParser(json))
    def decode(json: String): T = decode(factory.createParser(json))
    def decode(input: InputStream): T = decode(factory.createParser(input))
    def decode(input: Reader): T = decode(factory.createParser(input))

    private def decode(parser: JsonParser): T = {
      try {
        val value = reader.readValue[T](parser)
        require(parser.nextToken() == null, "invalid json, additional content after value")
        value
      } finally {
        parser.close()
      }
    }
  }

  private val jsonFactory = setup(new JsonFactory)
  private val smileFactory = setup(new SmileFactory)

  private val jsonMapper = newMapper(jsonFactory)

  private val smileMapper = newMapper(smileFactory)

  private def setup(factory: JsonFactory): JsonFactory = {
    factory.enable(ALLOW_COMMENTS)
    factory.enable(ALLOW_NON_NUMERIC_NUMBERS)
    factory.enable(AUTO_CLOSE_SOURCE)
    factory.enable(AUTO_CLOSE_TARGET)
    factory.disable(QUOTE_NON_NUMERIC_NUMBERS)
  }

  private def newMapper(factory: JsonFactory): ObjectMapper = {
    val mapper = new ObjectMapper(factory)
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    mapper.registerModule(new JodaModule)
    mapper
  }

  // Taken from com.fasterxml.jackson.module.scala.deser.DeserializerTest.scala
  private def typeReference[T: Manifest] = new TypeReference[T] {
    override def getType = typeFromManifest(manifest[T])
  }

  // Taken from com.fasterxml.jackson.module.scala.deser.DeserializerTest.scala
  private def typeFromManifest(m: Manifest[_]): Type = {
    if (m.typeArguments.isEmpty) { m.runtimeClass }
    else new ParameterizedType {
      def getRawType = m.runtimeClass

      def getActualTypeArguments = m.typeArguments.map(typeFromManifest).toArray

      def getOwnerType = null
    }
  }

  def newJsonGenerator(writer: Writer): JsonGenerator = {
    jsonFactory.createGenerator(writer)
  }

  def newJsonGenerator(stream: OutputStream): JsonGenerator = {
    jsonFactory.createGenerator(stream, JsonEncoding.UTF8)
  }

  def newJsonParser(reader: Reader): JsonParser = {
    jsonFactory.createParser(reader)
  }

  def newJsonParser(stream: InputStream): JsonParser = {
    jsonFactory.createParser(stream)
  }

  def newJsonParser(string: String): JsonParser = {
    jsonFactory.createParser(string)
  }

  def newJsonParser(bytes: Array[Byte]): JsonParser = {
    jsonFactory.createParser(bytes)
  }

  def newSmileGenerator(stream: OutputStream): JsonGenerator = {
    smileFactory.createGenerator(stream)
  }

  def newSmileParser(stream: InputStream): JsonParser = {
    smileFactory.createParser(stream)
  }

  def newSmileParser(bytes: Array[Byte]): JsonParser = {
    smileFactory.createParser(bytes, 0, bytes.length)
  }

  def encode[T: Manifest](obj: T): String = {
    jsonMapper.writeValueAsString(obj)
  }

  def encode[T: Manifest](writer: Writer, obj: T) {
    jsonMapper.writeValue(writer, obj)
  }

  def encode[T: Manifest](stream: OutputStream, obj: T) {
    jsonMapper.writeValue(stream, obj)
  }

  def encode[T: Manifest](gen: JsonGenerator, obj: T) {
    jsonMapper.writeValue(gen, obj)
  }

  def decodeResource[T: Manifest](name: String): T = {
    val url = getClass.getClassLoader.getResource(name)
    require(url != null, s"could not find resource: $name")
    val input = url.openStream()
    try decode[T](input) finally input.close()
  }

  def decode[T: Manifest](json: Array[Byte]): T = decoder[T].decode(json)

  def decode[T: Manifest](json: String): T = decoder[T].decode(json)

  def decode[T: Manifest](reader: Reader): T = decoder[T].decode(reader)

  def decode[T: Manifest](stream: InputStream): T = decoder[T].decode(stream)

  def decode[T: Manifest](parser: JsonParser): T = {
    val reader =
      if (manifest.runtimeClass.isArray)
        jsonMapper.reader(manifest.runtimeClass.asInstanceOf[Class[T]])
      else
        jsonMapper.reader(typeReference[T])
    val value = reader.readValue[T](parser)
    require(parser.nextToken() == null, "invalid json, additional content after value")
    value
  }

  def decoder[T: Manifest]: Decoder[T] = {
    val reader =
      if (manifest.runtimeClass.isArray)
        jsonMapper.reader(manifest.runtimeClass.asInstanceOf[Class[T]])
      else
        jsonMapper.reader(typeReference[T])
    new Decoder[T](reader, jsonFactory)
  }

  def smileEncode[T: Manifest](obj: T): Array[Byte] = {
    smileMapper.writeValueAsBytes(obj)
  }

  def smileEncode[T: Manifest](stream: OutputStream, obj: T) {
    smileMapper.writeValue(stream, obj)
  }

  def smileDecode[T: Manifest](stream: InputStream): T = smileDecoder[T].decode(stream)

  def smileDecode[T: Manifest](json: Array[Byte]): T = smileDecoder[T].decode(json)

  def smileDecoder[T: Manifest]: Decoder[T] = {
    val reader =
      if (manifest.runtimeClass.isArray)
        jsonMapper.reader(manifest.runtimeClass.asInstanceOf[Class[T]])
      else
        jsonMapper.reader(typeReference[T])
    new Decoder[T](reader, smileFactory)
  }
}
