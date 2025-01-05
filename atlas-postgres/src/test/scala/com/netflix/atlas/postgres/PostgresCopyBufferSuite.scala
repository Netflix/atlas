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
package com.netflix.atlas.postgres

import com.netflix.atlas.core.util.SortedTagMap
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres

import java.sql.Statement
import scala.util.Using
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import munit.FunSuite

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

class PostgresCopyBufferSuite extends FunSuite {

  private val buffers = List(
    "text"   -> new TextCopyBuffer(1024),
    "binary" -> new BinaryCopyBuffer(1024, 1)
  )

  private var postgres: EmbeddedPostgres = _
  private var connection: Connection = _
  private var copyManager: CopyManager = _

  override def beforeAll(): Unit = {
    postgres = EmbeddedPostgres
      .builder()
      .setCleanDataDirectory(true)
      .setPort(54321)
      .start()

    Class.forName("org.postgresql.Driver")
    connection = DriverManager.getConnection(
      "jdbc:postgresql://localhost:54321/postgres",
      "postgres",
      "postgres"
    )

    copyManager = connection.asInstanceOf[BaseConnection].getCopyAPI
  }

  override def afterAll(): Unit = {
    connection.close()
    postgres.close()
  }

  private val tableName = "copy_buffer_test"

  private def createTable(stmt: Statement, dataType: String): Unit = {
    stmt.executeUpdate("create extension if not exists hstore")
    stmt.executeUpdate(s"drop table if exists $tableName")
    stmt.executeUpdate(s"create table $tableName(value $dataType)")
  }

  private def getData[T](stmt: Statement, f: ResultSet => T): List[T] = {
    Using.resource(stmt.executeQuery(s"select value from $tableName")) { rs =>
      val builder = List.newBuilder[T]
      while (rs.next()) {
        builder += f(rs)
      }
      builder.result()
    }
  }

  private def stringTest(buffer: CopyBuffer, dataType: String): Unit = {
    buffer.clear()
    Using.resource(connection.createStatement()) { stmt =>
      createTable(stmt, dataType)
      assert(buffer.putString(null).nextRow())
      assert(buffer.putString("foo").nextRow())
      assert(buffer.putString("bar").nextRow())
      buffer.copyIn(copyManager, tableName)
      assertEquals(getData(stmt, _.getString(1)), List(null, "foo", "bar"))
    }
  }

  buffers.foreach {
    case (name, buffer) =>
      test(s"$name: varchar") {
        stringTest(buffer, "varchar(40)")
      }

      test(s"$name: text") {
        stringTest(buffer, "text")
      }

      test(s"$name: json") {
        buffer.clear()
        Using.resource(connection.createStatement()) { stmt =>
          createTable(stmt, "json")
          assert(buffer.putTagsJson(SortedTagMap.empty).nextRow())
          assert(buffer.putTagsJson(SortedTagMap("a" -> "1")).nextRow())
          assert(buffer.putTagsJson(SortedTagMap("a" -> "1", "b" -> "2")).nextRow())
          buffer.copyIn(copyManager, tableName)
          val expected = List(
            "{}",
            """{"a":"1"}""",
            """{"a":"1","b":"2"}"""
          )
          val actual = getData(stmt, _.getString(1))
          assertEquals(actual, expected)
        }
      }

      test(s"$name: jsonb") {
        buffer.clear()
        Using.resource(connection.createStatement()) { stmt =>
          createTable(stmt, "jsonb")
          assert(buffer.putTagsJsonb(SortedTagMap.empty).nextRow())
          assert(buffer.putTagsJsonb(SortedTagMap("a" -> "1")).nextRow())
          assert(buffer.putTagsJsonb(SortedTagMap("a" -> "1", "b" -> "2")).nextRow())
          buffer.copyIn(copyManager, tableName)
          val expected = List(
            "{}",
            """{"a": "1"}""",
            """{"a": "1", "b": "2"}"""
          )
          val actual = getData(stmt, _.getString(1))
          assertEquals(actual, expected)
        }
      }

      test(s"$name: hstore") {
        buffer.clear()
        Using.resource(connection.createStatement()) { stmt =>
          createTable(stmt, "hstore")
          assert(buffer.putTagsHstore(SortedTagMap.empty).nextRow())
          assert(buffer.putTagsHstore(SortedTagMap("a" -> "1")).nextRow())
          assert(buffer.putTagsHstore(SortedTagMap("a" -> "1", "b" -> "2")).nextRow())
          buffer.copyIn(copyManager, tableName)
          val expected = List(
            "",
            """"a"=>"1"""",
            """"a"=>"1", "b"=>"2""""
          )
          val actual = getData(stmt, _.getString(1))
          assertEquals(actual, expected)
        }
      }

      test(s"$name: smallint") {
        buffer.clear()
        Using.resource(connection.createStatement()) { stmt =>
          createTable(stmt, "smallint")
          assert(buffer.putShort(0).nextRow())
          assert(buffer.putShort(Short.MinValue).nextRow())
          assert(buffer.putShort(Short.MaxValue).nextRow())
          buffer.copyIn(copyManager, tableName)
          val expected = List(
            0.toShort,
            Short.MinValue,
            Short.MaxValue
          )
          val actual = getData(stmt, _.getShort(1))
          assertEquals(actual, expected)
        }
      }

      test(s"$name: integer") {
        buffer.clear()
        Using.resource(connection.createStatement()) { stmt =>
          createTable(stmt, "integer")
          assert(buffer.putInt(0).nextRow())
          assert(buffer.putInt(Int.MinValue).nextRow())
          assert(buffer.putInt(Int.MaxValue).nextRow())
          buffer.copyIn(copyManager, tableName)
          val expected = List(
            0,
            Int.MinValue,
            Int.MaxValue
          )
          val actual = getData(stmt, _.getInt(1))
          assertEquals(actual, expected)
        }
      }

      test(s"$name: bigint") {
        buffer.clear()
        Using.resource(connection.createStatement()) { stmt =>
          createTable(stmt, "bigint")
          assert(buffer.putLong(0L).nextRow())
          assert(buffer.putLong(Long.MinValue).nextRow())
          assert(buffer.putLong(Long.MaxValue).nextRow())
          buffer.copyIn(copyManager, tableName)
          val expected = List(
            0L,
            Long.MinValue,
            Long.MaxValue
          )
          val actual = getData(stmt, _.getLong(1))
          assertEquals(actual, expected)
        }
      }

      test(s"$name: double precision") {
        buffer.clear()
        Using.resource(connection.createStatement()) { stmt =>
          createTable(stmt, "double precision")
          assert(buffer.putDouble(0.0).nextRow())
          assert(buffer.putDouble(Double.MinValue).nextRow())
          assert(buffer.putDouble(Double.MaxValue).nextRow())
          assert(buffer.putDouble(Double.MinPositiveValue).nextRow())
          assert(buffer.putDouble(Double.NaN).nextRow())
          assert(buffer.putDouble(Double.NegativeInfinity).nextRow())
          assert(buffer.putDouble(Double.PositiveInfinity).nextRow())
          buffer.copyIn(copyManager, tableName)
          val expected = List(
            0.0,
            Double.MinValue,
            Double.MaxValue,
            Double.MinPositiveValue,
            Double.NaN,
            Double.NegativeInfinity,
            Double.PositiveInfinity
          )
          val actual = getData(stmt, _.getDouble(1))
          actual.zip(expected).foreach {
            case (a, e) => if (a.isNaN) assert(e.isNaN) else assertEquals(a, e)
          }
        }
      }

      test(s"$name: double precision[]") {
        buffer.clear()
        Using.resource(connection.createStatement()) { stmt =>
          createTable(stmt, "double precision[]")
          val expected = List(
            0.0,
            Double.MinValue,
            Double.MaxValue,
            Double.MinPositiveValue,
            Double.NaN,
            Double.NegativeInfinity,
            Double.PositiveInfinity
          )
          assert(buffer.putDoubleArray(expected.toArray).nextRow())
          buffer.copyIn(copyManager, tableName)
          val actual = getData(stmt, _.getArray(1).getArray.asInstanceOf[Array[java.lang.Double]])
          assertEquals(actual.size, 1)
          actual.foreach { data =>
            data.toList.zip(expected).foreach {
              case (a, e) => if (a.isNaN) assert(e.isNaN) else assertEquals(a.doubleValue(), e)
            }
          }
        }
      }

  }
}
