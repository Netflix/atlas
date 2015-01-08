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
package com.netflix.atlas.core.util

object ArrayHelper {

  import java.util.{Arrays => JArrays}

  def fill(size: Int, value: Double): Array[Double] = {
    val array = new Array[Double](size)
    JArrays.fill(array, value)
    array
  }

  def fill(size: Int, value: Float): Array[Float] = {
    val array = new Array[Float](size)
    JArrays.fill(array, value)
    array
  }

  def fill(size: Int, value: Int): Array[Int] = {
    val array = new Array[Int](size)
    JArrays.fill(array, value)
    array
  }
}
