/*
 * Copyright 2014-2021 Netflix, Inc.
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

import scala.reflect.ClassTag

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

  def newInstance[T](n: Int): Array[T] = {

    // Note, we do a cast here and avoid using RefIntHashMap[T: ClassTag] because
    // the ClassTag seems to add a lot of memory overhead. From jol when using
    // ClassTag:
    //
    // com.netflix.atlas.core.util.RefIntHashMap@78e94dcfd footprint:
    // COUNT       AVG       SUM   DESCRIPTION
    //    55        50      2760   [C
    //     1        64        64   [I
    //     4        20        80   [Ljava.lang.Class;
    //     1        64        64   [Ljava.lang.Long;
    //     3        40       120   [Ljava.lang.Object;
    //     9        35       320   [Ljava.lang.reflect.Field;
    //     2        16        32   [Ljava.lang.reflect.Method;
    //     1        24        24   [Ljava.lang.reflect.TypeVariable;
    //     1        16        16   [Lsun.reflect.generics.tree.ClassTypeSignature;
    //     2        24        48   [Lsun.reflect.generics.tree.FieldTypeSignature;
    //     1        24        24   [Lsun.reflect.generics.tree.FormalTypeParameter;
    //     3        16        48   [Lsun.reflect.generics.tree.TypeArgument;
    //     1        32        32   com.netflix.atlas.core.util.RefIntHashMap
    //    22       525     11560   java.lang.Class
    //     7        56       392   java.lang.Class$ReflectionData
    //     5        24       120   java.lang.Long
    //    55        24      1320   java.lang.String
    //     1        16        16   java.lang.ref.ReferenceQueue$Lock
    //     1        32        32   java.lang.ref.ReferenceQueue$Null
    //     7        40       280   java.lang.ref.SoftReference
    //    52        72      3744   java.lang.reflect.Field
    //     3        24        72   java.util.ArrayList
    //     1        16        16   scala.reflect.ClassTag$$anon$1
    //     2        32        64   sun.reflect.UnsafeObjectFieldAccessorImpl
    //     9        32       288   sun.reflect.UnsafeQualifiedObjectFieldAccessorImpl
    //     1        40        40   sun.reflect.UnsafeQualifiedStaticObjectFieldAccessorImpl
    //     1        24        24   sun.reflect.generics.factory.CoreReflectionFactory
    //     2        32        64   sun.reflect.generics.reflectiveObjects.TypeVariableImpl
    //     1        32        32   sun.reflect.generics.repository.ClassRepository
    //     1        24        24   sun.reflect.generics.scope.ClassScope
    //     1        24        24   sun.reflect.generics.tree.ClassSignature
    //     3        16        48   sun.reflect.generics.tree.ClassTypeSignature
    //     2        24        48   sun.reflect.generics.tree.FormalTypeParameter
    //     3        24        72   sun.reflect.generics.tree.SimpleClassTypeSignature
    //   264               21912   (total)
    new Array[AnyRef](n).asInstanceOf[Array[T]]
  }

  /**
    * Creates a merger object that can be used to efficiently merge together sorted arrays
    * of comparable objects.
    *
    * @param limit
    *     The maximum number of items allowed in the result list. The merge operations will
    *     be stopped as soon as the limit is reached.
    * @return
    *     Object that can be used to merge the arrays.
    */
  def merger[T <: Comparable[T]: ClassTag](limit: Int): Merger[T] = new Merger[T](limit)

  /** Helper for merging sorted arrays and lists. */
  class Merger[T <: Comparable[T]: ClassTag] private[util] (limit: Int) {
    // Arrays used for storing the merged result. The `src` array will contain the
    // current merged dataset. During a merge operation, the data will be written
    // into the destination array. It is pre-allocated so it can be reused across
    // many merger operations.
    private var src = new Array[T](limit)
    private var dst = new Array[T](limit)

    /** Number of elements in the merged result. */
    var size = 0

    /** Merge a sorted array with the current data set. */
    def merge(vs: Array[T]): Merger[T] = merge(vs, vs.length)

    /** Merge a sorted array with the current data set. */
    def merge(vs: Array[T], vsize: Int): Merger[T] = {
      // Source, merge array, and destination indices
      var sidx = 0
      var vidx = 0
      var didx = 0

      // While both have data, merge and dedup
      while (sidx < size && vidx < vsize && didx < limit) {
        val v1 = src(sidx)
        val v2 = vs(vidx)
        v1.compareTo(v2) match {
          case c if c < 0 =>
            dst(didx) = v1
            didx += 1
            sidx += 1
          case c if c == 0 =>
            dst(didx) = v1
            didx += 1
            sidx += 1
            vidx += 1
          case c if c > 0 =>
            dst(didx) = v2
            didx += 1
            vidx += 1
        }
      }

      // Only source has data left, fill in the remainder
      if (sidx < size && didx < limit) {
        val length = math.min(limit - didx, size - sidx)
        System.arraycopy(src, sidx, dst, didx, length)
        didx += length
      }

      // Only the merge array has data left, fill in the remainder
      if (vidx < vsize && didx < limit) {
        val length = math.min(limit - didx, vsize - vidx)
        System.arraycopy(vs, vidx, dst, didx, length)
        didx += length
      }

      size = didx

      // Swap the buffers
      val tmp = src
      src = dst
      dst = tmp

      this
    }

    def merge(vs: List[T]): Merger[T] = {
      // Source and destination indices
      var sidx = 0
      var didx = 0

      // Pointer to head of merge list
      var data = vs

      // While both have data, merge and dedup
      while (sidx < size && data.nonEmpty && didx < limit) {
        val v1 = src(sidx)
        val v2 = data.head
        v1.compareTo(v2) match {
          case c if c < 0 =>
            dst(didx) = v1
            didx += 1
            sidx += 1
          case c if c == 0 =>
            dst(didx) = v1
            didx += 1
            sidx += 1
            data = data.tail
          case c if c > 0 =>
            dst(didx) = v2
            didx += 1
            data = data.tail
        }
      }

      // Only source has data left, fill in the remainder
      if (sidx < size && didx < limit) {
        val length = math.min(limit - didx, size - sidx)
        System.arraycopy(src, sidx, dst, didx, length)
        didx += length
      }

      // Only the merge list has data left, fill in the remainder
      while (data.nonEmpty && didx < limit) {
        val iter = data.iterator
        while (iter.hasNext && didx < limit) {
          dst(didx) = iter.next()
          didx += 1
        }
      }

      size = didx

      // Swap the buffers
      val tmp = src
      src = dst
      dst = tmp

      this
    }

    /** Return merged array of at most `limit` elements. */
    def toArray: Array[T] = {
      val data = new Array[T](size)
      System.arraycopy(src, 0, data, 0, size)
      data
    }

    /** Return merged list of at most `limit` elements. */
    def toList: List[T] = {
      val builder = List.newBuilder[T]
      var i = 0
      while (i < size) {
        builder += src(i)
        i += 1
      }
      builder.result()
    }
  }
}
