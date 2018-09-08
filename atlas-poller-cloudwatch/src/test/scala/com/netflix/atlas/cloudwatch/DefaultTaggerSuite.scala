/*
 * Copyright 2014-2018 Netflix, Inc.
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
package com.netflix.atlas.cloudwatch

import java.util.regex.PatternSyntaxException

import com.amazonaws.services.cloudwatch.model.Dimension
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class DefaultTaggerSuite extends FunSuite {

  private val dimensions = List(
    new Dimension().withName("CloudWatch").withValue("abc"),
    new Dimension().withName("ExtractDotDelimited").withValue("abc.def.captured-portion"),
    new Dimension().withName("ExtractSlashDelimited").withValue("abc/captured-portion/42beef9876"),
    new Dimension().withName("NoMapping").withValue("def")
  )

  test("bad config") {
    val cfg = ConfigFactory.parseString("")
    intercept[ConfigException] {
      new DefaultTagger(cfg)
    }
  }

  test("add common tags") {
    val cfg = ConfigFactory.parseString("""
        |extractors = []
        |mappings = []
        |common-tags = [
        |  {
        |    key = "foo"
        |    value = "bar"
        |  }
        |]
      """.stripMargin)

    val expected = Map(
      "foo"                   -> "bar",
      "CloudWatch"            -> "abc",
      "ExtractDotDelimited"   -> "abc.def.captured-portion",
      "ExtractSlashDelimited" -> "abc/captured-portion/42beef9876",
      "NoMapping"             -> "def"
    )

    val tagger = new DefaultTagger(cfg)
    assert(tagger(dimensions) === expected)
  }

  test("apply key mappings") {
    val cfg = ConfigFactory.parseString("""
        |extractors = []
        |mappings = [
        |  {
        |    name = "CloudWatch"
        |    alias = "InternalAlias"
        |  }
        |]
        |common-tags = []
      """.stripMargin)

    val expected = Map(
      "ExtractDotDelimited"   -> "abc.def.captured-portion",
      "ExtractSlashDelimited" -> "abc/captured-portion/42beef9876",
      "InternalAlias"         -> "abc",
      "NoMapping"             -> "def"
    )

    val tagger = new DefaultTagger(cfg)
    assert(tagger(dimensions) === expected)
  }

  test("extract value for configured keys") {
    val cfg = ConfigFactory.parseString("""
        |extractors = [
        |  {
        |    name = "ExtractDotDelimited"
        |    pattern = "[^.]+\\.[^.]+\\.(.*)"
        |  },
        |  {
        |    name = "ExtractSlashDelimited"
        |    pattern = "[^.]+/([^.]+)/.*"
        |  }
        |]
        |mappings = []
        |common-tags = []
      """.stripMargin)

    val expected = Map(
      "ExtractDotDelimited"   -> "captured-portion",
      "ExtractSlashDelimited" -> "captured-portion",
      "CloudWatch"            -> "abc",
      "NoMapping"             -> "def"
    )

    val tagger = new DefaultTagger(cfg)
    assert(tagger(dimensions) === expected)
  }

  test("syntax error in extractor pattern throws") {
    val cfg = ConfigFactory.parseString("""
        |extractors = [
        |  {
        |    name = "ExtractDotDelimited"
        |    pattern = "[^.]+\\.[^.]+\\.(.*"
        |  }
        |]
        |mappings = []
        |common-tags = []
      """.stripMargin)

    intercept[PatternSyntaxException] {
      new DefaultTagger(cfg)
    }

  }

  test("extractor without capture group returns raw value") {
    val cfg = ConfigFactory.parseString("""
        |extractors = [
        |  {
        |    name = "ExtractDotDelimited"
        |    pattern = "[^.]+\\.[^.]+\\..*"
        |  }
        |]
        |mappings = []
        |common-tags = []
      """.stripMargin)

    val expected = Map(
      "ExtractDotDelimited"   -> "abc.def.captured-portion",
      "ExtractSlashDelimited" -> "abc/captured-portion/42beef9876",
      "CloudWatch"            -> "abc",
      "NoMapping"             -> "def"
    )

    val tagger = new DefaultTagger(cfg)
    assert(tagger(dimensions) === expected)
  }

  test("apply key mappings and extract value for configured keys") {
    val cfg = ConfigFactory.parseString("""
        |extractors = [
        |  {
        |    name = "ExtractDotDelimited"
        |    pattern = "[^.]+\\.[^.]+\\.(.*)"
        |  },
        |  {
        |    name = "ExtractSlashDelimited"
        |    pattern = "[^.]+/([^.]+)/.*"
        |  }
        |]
        |mappings = [
        |  {
        |    name = "ExtractSlashDelimited"
        |    alias = "extracted"
        |  }
        |]
        |common-tags = []
      """.stripMargin)

    val expected = Map(
      "ExtractDotDelimited" -> "captured-portion",
      "extracted"           -> "captured-portion",
      "CloudWatch"          -> "abc",
      "NoMapping"           -> "def"
    )

    val tagger = new DefaultTagger(cfg)
    assert(tagger(dimensions) === expected)
  }

  test("dimensions override common tags") {
    val cfg = ConfigFactory.parseString("""
        |extractors = []
        |mappings = [
        |  {
        |    name = "CloudWatch"
        |    alias = "foo"
        |  }
        |]
        |common-tags = [
        |  {
        |    key = "foo"
        |    value = "bar"
        |  }
        |]
      """.stripMargin)

    val expected = Map(
      "ExtractDotDelimited"   -> "abc.def.captured-portion",
      "ExtractSlashDelimited" -> "abc/captured-portion/42beef9876",
      "foo"                   -> "abc",
      "NoMapping"             -> "def"
    )

    val tagger = new DefaultTagger(cfg)
    assert(tagger(dimensions) === expected)
  }
}
