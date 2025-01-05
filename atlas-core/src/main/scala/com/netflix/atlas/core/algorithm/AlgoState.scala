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
package com.netflix.atlas.core.algorithm

/**
  * Represents the current state for an online algorithm. Can be used with `OnlineAlgorithm.apply`
  * to create a new instance with the same state. This state object is reliably serializable with
  * helpers such as the `atlas-json` library so state can be persisted.
  */
case class AlgoState(algorithm: String, settings: Map[String, Any]) {

  private def get[T](key: String): T = settings(key).asInstanceOf[T]

  /**
    * Handle conversion of value to a number. This is needed if special double values are
    * used that cannot be represented in JSON (NaN, Infinity).
    *
    * @param key
    *     Used to provide more context in the exception if there is a failure.
    * @param value
    *     Raw value for the key that needs to be converted to a number.
    * @return
    *     Numeric value for the key.
    */
  private def toNumber(key: String, value: Any): Number = {
    value match {
      case v: Number => v
      case v: String => java.lang.Double.parseDouble(v)
      case v         => throw new IllegalStateException(s"$key has non-numeric type: ${v.getClass}")
    }
  }

  /** Retrieve a boolean value for a given key. */
  def getBoolean(key: String): Boolean = get[Boolean](key)

  /** Retrieve a number value for a given key. */
  def getNumber(key: String): Number = toNumber(key, settings(key))

  /** Retrieve an integer value for a given key. */
  def getInt(key: String): Int = getNumber(key).intValue()

  /** Retrieve a long value for a given key. */
  def getLong(key: String): Long = getNumber(key).longValue()

  /** Retrieve a double value for a given key. */
  def getDouble(key: String): Double = getNumber(key).doubleValue()

  /** Retrieve a string value for a given key. */
  def getString(key: String): String = get[String](key)

  /** Retrieve an array of double values for a given key. */
  def getDoubleArray(key: String): Array[Double] = {
    // When the state is from a de-serialized source the type may have changed to
    // a Seq type such as List.
    settings(key) match {
      case vs: Seq[?]        => vs.map(v => toNumber(key, v).doubleValue()).toArray
      case vs: Array[Double] => vs
      case v                 => throw new MatchError(v)
    }
  }

  /** Retrieve a sub-state object for a given key. */
  def getState(key: String): AlgoState = AlgoState(settings(key))

  /** Retrieve a list of sub-state objects for a given key. */
  def getStateList(key: String): List[AlgoState] = get[List[Any]](key).map(AlgoState.apply)
}

/** Helper functions to make it easier to create state objects. */
object AlgoState {

  /** Create a new instance. */
  def apply(algorithm: String, settings: (String, Any)*): AlgoState = {
    apply(algorithm, settings.toMap)
  }

  private def apply(map: Map[?, ?]): AlgoState = {
    val m = map.asInstanceOf[Map[String, Any]]
    apply(m("algorithm").asInstanceOf[String], m("settings").asInstanceOf[Map[String, Any]])
  }

  private def apply(value: Any): AlgoState = {
    value match {
      case s: AlgoState => s
      case m: Map[?, ?] => apply(m)
      case v            => throw new MatchError(v)
    }
  }
}
