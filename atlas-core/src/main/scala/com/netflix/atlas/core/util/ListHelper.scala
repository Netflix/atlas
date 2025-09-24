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

/**
  * Helper functions for working with lists.
  */
object ListHelper {

  /**
    * Merge and dedup two sorted lists up to the specified limit. The input lists must already
    * be sorted and should not contain duplicate values.
    *
    * @param limit
    *     Maximum number of items in the resulting list.
    * @param v1
    *     A sorted list to merge.
    * @param v2
    *     A sorted list to merge.
    * @return
    *     Sorted list with a max size of `limit`.
    */
  def merge[T <: Comparable[T]](limit: Int, v1: List[T], v2: List[T]): List[T] = {
    merge(limit, new ComparableComparator[T], v1, v2)
  }

  /**
    * Merge and dedup two sorted lists up to the specified limit. The input lists must already
    * be sorted and should not contain duplicate values.
    *
    * @param limit
    *     Maximum number of items in the resulting list.
    * @param comparator
    *     Comparator to use for determining the order of elements.
    * @param v1
    *     A sorted list to merge.
    * @param v2
    *     A sorted list to merge.
    * @return
    *     Sorted list with a max size of `limit`.
    */
  def merge[T](limit: Int, comparator: Comparator[T], v1: List[T], v2: List[T]): List[T] = {
    merge(limit, comparator, (v: T, _: T) => v, v1, v2)
  }

  /**
    * Merge and dedup two sorted lists up to the specified limit. The input lists must already
    * be sorted and should not contain duplicate values.
    *
    * @param limit
    *     Maximum number of items in the resulting list.
    * @param comparator
    *     Comparator to use for determining the order of elements.
    * @param aggrF
    *     Aggregation function to use if duplicate values are encountered. The user should
    *     ensure that the aggregation function does not influence the order of the elements.
    * @param v1
    *     A sorted list to merge.
    * @param v2
    *     A sorted list to merge.
    * @return
    *     Sorted list with a max size of `limit`.
    */
  def merge[T](
    limit: Int,
    comparator: Comparator[T],
    aggrF: (T, T) => T,
    v1: List[T],
    v2: List[T]
  ): List[T] = {
    mergeImpl(limit, comparator, aggrF, 0, Nil, v1, v2)
  }

  @scala.annotation.tailrec
  private def mergeImpl[T](
    limit: Int,
    comparator: Comparator[T],
    aggrF: (T, T) => T,
    size: Int,
    acc: List[T],
    v1: List[T],
    v2: List[T]
  ): List[T] = {
    if (size == limit)
      acc.reverse
    else if (v1.isEmpty)
      acc.reverse ++ v2.take(limit - size)
    else if (v2.isEmpty)
      acc.reverse ++ v1.take(limit - size)
    else
      comparator.compare(v1.head, v2.head) match {
        case c if c < 0 =>
          mergeImpl(limit, comparator, aggrF, size + 1, v1.head :: acc, v1.tail, v2)
        case c if c == 0 =>
          val v = aggrF(v1.head, v2.head)
          mergeImpl(limit, comparator, aggrF, size + 1, v :: acc, v1.tail, v2.tail)
        case _ =>
          mergeImpl(limit, comparator, aggrF, size + 1, v2.head :: acc, v1, v2.tail)
      }
  }

  /**
    * Merge and dedup sorted lists up to the specified limit. The input lists must already
    * be sorted and should not contain duplicate values.
    *
    * @param limit
    *     Maximum number of items in the resulting list.
    * @param vs
    *     A list of sorted lists to merge.
    * @return
    *     Sorted list with a max size of `limit`.
    */
  def merge[T <: Comparable[T]: ClassTag](limit: Int, vs: List[List[T]]): List[T] = {
    merge(limit, new ComparableComparator[T], vs)
  }

  /**
    * Merge and dedup sorted lists up to the specified limit. The input lists must already
    * be sorted and should not contain duplicate values.
    *
    * @param limit
    *     Maximum number of items in the resulting list.
    * @param comparator
    *     Comparator to use for determining the order of elements.
    * @param vs
    *     A list of sorted lists to merge.
    * @return
    *     Sorted list with a max size of `limit`.
    */
  def merge[T: ClassTag](limit: Int, comparator: Comparator[T], vs: List[List[T]]): List[T] = {
    merge(limit, comparator, (v: T, _: T) => v, vs)
  }

  /**
    * Merge and dedup sorted lists up to the specified limit. The input lists must already
    * be sorted and should not contain duplicate values.
    *
    * @param limit
    *     Maximum number of items in the resulting list.
    * @param comparator
    *     Comparator to use for determining the order of elements.
    * @param aggrF
    *     Aggregation function to use if duplicate values are encountered. The user should
    *     ensure that the aggregation function does not influence the order of the elements.
    * @param vs
    *     A list of sorted lists to merge.
    * @return
    *     Sorted list with a max size of `limit`.
    */
  def merge[T: ClassTag](
    limit: Int,
    comparator: Comparator[T],
    aggrF: (T, T) => T,
    vs: List[List[T]]
  ): List[T] = {
    val merged = vs.foldLeft(ArrayHelper.merger[T](limit, comparator, aggrF)) { (m, vs) =>
      m.merge(vs)
    }
    merged.toList
  }
}
