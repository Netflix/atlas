import java.io.File
import java.io.PrintStream
import java.time.ZonedDateTime
import java.time.ZoneOffset
import scala.io.Source
import sbt.*

import scala.util.Using

/**
  * Loosely based on: https://github.com/Banno/sbt-license-plugin
  *
  * Main changes in functionality:
  * - remove spurious whitespace on empty lines for license
  * - supports both test and main source files
  * - add target to check which can fail the build
  */
object LicenseCheck {
  private val lineSeparator = java.lang.System.lineSeparator()

  def year: Int = ZonedDateTime.now(ZoneOffset.UTC).getYear

  val apache2: String = s"""
   |/*
   | * Copyright 2014-$year Netflix, Inc.
   | *
   | * Licensed under the Apache License, Version 2.0 (the "License");
   | * you may not use this file except in compliance with the License.
   | * You may obtain a copy of the License at
   | *
   | *     http://www.apache.org/licenses/LICENSE-2.0
   | *
   | * Unless required by applicable law or agreed to in writing, software
   | * distributed under the License is distributed on an "AS IS" BASIS,
   | * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   | * See the License for the specific language governing permissions and
   | * limitations under the License.
   | */
  """.stripMargin.trim

  def findFiles(dir: File): Seq[File] = {
    (dir ** "*.scala").get ++ (dir ** "*.java").get
  }

  def checkLicenseHeaders(log: Logger, srcDir: File): Unit = {
    val badFiles = findFiles(srcDir).filterNot(checkLicenseHeader)
    if (badFiles.nonEmpty) {
      badFiles.foreach { f =>
        log.error(s"bad license header: $f")
      }
      sys.error(s"${badFiles.size} files with incorrect header, run formatLicenseHeaders to fix")
    } else {
      log.info("all files have correct license header")
    }
  }

  def checkLicenseHeader(file: File): Boolean = {
    val lines = Using.resource(Source.fromFile(file, "UTF-8"))(_.getLines().toList)
    checkLicenseHeader(lines)
  }

  def checkLicenseHeader(lines: List[String]): Boolean = {
    val header = lines.takeWhile(!_.startsWith("package ")).mkString(lineSeparator)
    header == apache2
  }

  def formatLicenseHeaders(log: Logger, srcDir: File): Unit = {
    findFiles(srcDir).foreach { f =>
      formatLicenseHeader(log, f)
    }
  }

  def formatLicenseHeader(log: Logger, file: File): Unit = {
    val lines = Using.resource(Source.fromFile(file, "UTF-8"))(_.getLines().toList)
    if (!checkLicenseHeader(lines)) {
      log.info(s"fixing license header: $file")
      writeLines(file, apache2 :: removeExistingHeader(lines))
    }
  }

  def removeExistingHeader(lines: List[String]): List[String] = {
    val res = lines.dropWhile(!_.startsWith("package "))
    if (res.isEmpty) lines else res
  }

  def writeLines(file: File, lines: List[String]): Unit = {
    val out = new PrintStream(file)
    try lines.foreach(out.println)
    finally out.close()
  }
}
