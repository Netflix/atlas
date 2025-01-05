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

import java.util.Comparator
import scala.reflect.ClassTag

object ArrayHelper {

  import java.util.Arrays as JArrays

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
  def merger[T <: Comparable[T]: ClassTag](limit: Int): Merger[T] = {
    merger(limit, new ComparableComparator[T])
  }

  /**
    * Creates a merger object that can be used to efficiently merge together sorted arrays
    * of comparable objects.
    *
    * @param limit
    *     The maximum number of items allowed in the result list. The merge operations will
    *     be stopped as soon as the limit is reached.
    * @param comparator
    *     Comparator to use for determining the order of elements.
    * @return
    *     Object that can be used to merge the arrays.
    */
  def merger[T: ClassTag](limit: Int, comparator: Comparator[T]): Merger[T] = {
    merger(limit, comparator, (v, _) => v)
  }

  /**
    * Creates a merger object that can be used to efficiently merge together sorted arrays
    * of comparable objects.
    *
    * @param limit
    *     The maximum number of items allowed in the result list. The merge operations will
    *     be stopped as soon as the limit is reached.
    * @param comparator
    *     Comparator to use for determining the order of elements.
    * @param aggrF
    *     Aggregation function to use if duplicate values are encountered. The user should
    *     ensure that the aggregation function does not influence the order of the elements.
    * @return
    *     Object that can be used to merge the arrays.
    */
  def merger[T: ClassTag](limit: Int, comparator: Comparator[T], aggrF: (T, T) => T): Merger[T] = {
    new Merger[T](limit, comparator, aggrF)
  }

  /** Helper for merging sorted arrays and lists. */
  class Merger[T: ClassTag] private[util] (
    limit: Int,
    comparator: Comparator[T],
    aggrF: (T, T) => T
  ) {

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
      size = ArrayHelper.merge(comparator, aggrF, src, size, vs, vsize, dst)

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
        comparator.compare(v1, v2) match {
          case c if c < 0 =>
            dst(didx) = v1
            didx += 1
            sidx += 1
          case c if c == 0 =>
            dst(didx) = aggrF(v1, v2)
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
        dst(didx) = data.head
        data = data.tail
        didx += 1
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

  /**
    * Merge two source arrays into a specified destination array.
    *
    * @param comparator
    *     Comparator to use for determining the order of elements.
    * @param aggrF
    *     Aggregation function to use if duplicate values are encountered. The user should
    *     ensure that the aggregation function does not influence the order of the elements.
    * @param vs1
    *     First source array, may be modified. The first position will be set to null if the
    *     contents of this array have been fully merged into the destination.
    * @param vs1size
    *     Number of valid elements in the first source array.
    * @param vs2
    *     Second source array, may be modified. The first position will be set to null if the
    *     contents of this array have been fully merged into the destination.
    * @param vs2size
    *     Number of valid elements in the second source array.
    * @param dst
    *     Destination array that will receive the merged data.
    * @return
    *     Number of valid elements in the merged array.
    */
  def merge[T](
    comparator: Comparator[T],
    aggrF: (T, T) => T,
    vs1: Array[T],
    vs1size: Int,
    vs2: Array[T],
    vs2size: Int,
    dst: Array[T]
  ): Int = {
    val limit = dst.length

    // Source 1, source 2, and destination indices
    var vs1idx = 0
    var vs2idx = 0
    var didx = 0

    // While both have data, merge and dedup
    while (vs1idx < vs1size && vs2idx < vs2size && didx < limit) {
      val v1 = vs1(vs1idx)
      val v2 = vs2(vs2idx)
      comparator.compare(v1, v2) match {
        case c if c < 0 =>
          dst(didx) = v1
          didx += 1
          vs1idx += 1
        case c if c == 0 =>
          dst(didx) = aggrF(v1, v2)
          didx += 1
          vs1idx += 1
          vs2idx += 1
        case c if c > 0 =>
          dst(didx) = v2
          didx += 1
          vs2idx += 1
      }
    }

    // Only source has data left, fill in the remainder
    if (vs1idx < vs1size && didx < limit) {
      val length = math.min(limit - didx, vs1size - vs1idx)
      System.arraycopy(vs1, vs1idx, dst, didx, length)
      vs1idx += length
      didx += length
    }

    // Update first position of source array with null if fully consumed
    if (vs1idx >= vs1size && vs1size > 0) {
      vs1(0) = null.asInstanceOf[T]
    }

    // Only the merge array has data left, fill in the remainder
    if (vs2idx < vs2size && didx < limit) {
      val length = math.min(limit - didx, vs2size - vs2idx)
      System.arraycopy(vs2, vs2idx, dst, didx, length)
      vs2idx += length
      didx += length
    }

    // Update first position of merge array with null if fully consumed
    if (vs2idx >= vs2size && vs2size > 0) {
      vs2(0) = null.asInstanceOf[T]
    }

    // Final output size
    didx
  }

  /**
    * Sort array and remove duplicate values from the array. The operations will be done in
    * place and modify the array.
    *
    * @param data
    *     Input data to sort. The array should be full.
    * @return
    *     Length of the valid data in the array after removing duplicates.
    */
  def sortAndDedup[T <: Comparable[T]](data: Array[T]): Int = {
    sortAndDedup(new ComparableComparator[T], (v: T, _: T) => v, data, data.length)
  }

  /**
    * Sort array and remove duplicate values from the array. The operations will be done in
    * place and modify the array.
    *
    * @param comparator
    *     Comparator to use for determining the order of elements.
    * @param aggrF
    *     Aggregation function to use if duplicate values are encountered. The user should
    *     ensure that the aggregation function does not influence the order of the elements.
    * @param data
    *     Input data to sort.
    * @param length
    *     Amount of data in the array to consider for the sort.
    * @return
    *     Length of the valid data in the array after removing duplicates.
    */
  def sortAndDedup[T](
    comparator: Comparator[T],
    aggrF: (T, T) => T,
    data: Array[T],
    length: Int
  ): Int = {
    if (length == 0) {
      0
    } else {
      java.util.Arrays.sort(
        data.asInstanceOf[Array[AnyRef]],
        0,
        length,
        comparator.asInstanceOf[Comparator[AnyRef]]
      )
      var v = data(0)
      var i = 1
      var j = 0
      while (i < length) {
        if (comparator.compare(v, data(i)) != 0) {
          j += 1
          v = data(i)
          data(j) = v
        } else {
          v = aggrF(v, data(i))
          data(j) = v
        }
        i += 1
      }
      j + 1
    }
  }

  /**
    * Sort a string array that consists of tag key/value pairs by key. The array will be
    * sorted in-place. The pair arrays are supposed to be fairly small, typically less than 20
    * tags. With the small size a simple insertion sort works well.
    */
  def sortPairs[T <: Comparable[T]](data: Array[T]): Unit = {
    sortPairs(data, data.length)
  }

  /**
    * Sort a string array that consists of tag key/value pairs by key. The array will be
    * sorted in-place. The pair arrays are supposed to be fairly small, typically less than 20
    * tags. With the small size a simple insertion sort works well.
    */
  def sortPairs[T <: Comparable[T]](data: Array[T], length: Int): Unit = {
    require(length % 2 == 0, "array must have even number of elements")
    if (length == 4) {
      // Two key/value pairs, swap if needed
      if (data(0).compareTo(data(2)) > 0) {
        // Swap key
        var tmp = data(0)
        data(0) = data(2)
        data(2) = tmp
        // Swap value
        tmp = data(1)
        data(1) = data(3)
        data(3) = tmp
      }
    } else if (length > 4) {
      // One entry is already sorted. Two entries handled above, for larger arrays
      // use insertion sort.
      var i = 2
      while (i < length) {
        val k = data(i)
        val v = data(i + 1)
        var j = i - 2

        while (j >= 0 && data(j).compareTo(k) > 0) {
          data(j + 2) = data(j)
          data(j + 3) = data(j + 1)
          j -= 2
        }
        data(j + 2) = k
        data(j + 3) = v

        i += 2
      }
    }
  }
}
