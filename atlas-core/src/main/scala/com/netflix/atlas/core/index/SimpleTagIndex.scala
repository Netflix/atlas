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
package com.netflix.atlas.core.index

import java.math.BigInteger

import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.Tag
import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.model.TaggedItem

import scala.reflect.ClassTag


class SimpleTagIndex[T <: TaggedItem: ClassTag](items: Array[T]) extends TagIndex[T] {

  type ValueMap = Map[String, Set[Int]]
  type KeyMap = Map[String, ValueMap]

  private val all = (0 until items.length).toSet

  private val index: KeyMap = init()

  private def init(): KeyMap = {
    items.zipWithIndex.foldLeft(Map.empty[String, ValueMap]) { (iAcc, t) =>
      val item = t._1
      val i = t._2
      item.tags.foldLeft(iAcc) { (kAcc, tag) =>
        val vAcc = kAcc.get(tag._1) match {
          case Some(vm) => vm.get(tag._2) match {
            case Some(vs) => vm + (tag._2 -> (vs + i))
            case None     => vm + (tag._2 -> Set(i))
          }
          case None => Map(tag._2 -> Set(i))
        }
        kAcc + (tag._1 -> vAcc)
      }
    }
  }

  private def findImpl(query: Query): Set[Int] = {
    import com.netflix.atlas.core.model.Query._
    query match {
      case And(q1, q2) =>
        findImpl(q1).intersect(findImpl(q2))
      case Or(q1, q2) =>
        findImpl(q1).union(findImpl(q2))
      case Not(q) =>
        all.diff(findImpl(q))
      case Equal(k, v) =>
        index.get(k) match {
          case Some(vidx) => vidx.getOrElse(v, Set.empty[Int])
          case None       => Set.empty[Int]
        }
      case q: KeyValueQuery =>
        index.get(q.k) match {
          case Some(vidx) =>
            vidx.foldLeft(Set.empty[Int]) { (acc, v) =>
              if (q.check(v._1)) acc.union(v._2) else acc
            }
          case None => Set.empty[Int]
        }
      case HasKey(k) =>
        index.get(k) match {
          case Some(vidx) =>
            vidx.foldLeft(Set.empty[Int]) { (acc, v) => acc.union(v._2) }
          case None => Set.empty[Int]
        }
      case True  => all
      case False => Set.empty[Int]
    }
  }

  private def findItemsImpl(query: Option[Query]): List[T] = {
    query.fold(items.toList)(q => findImpl(q).map(items).toList)
  }

  def findTags(query: TagQuery): List[Tag] = {
    val matches = findItemsImpl(query.query)
    val uniq = matches.flatMap(_.tags.map(t => Tag(t._1, t._2))).distinct

    val forKey = query.key.fold(uniq)(k => uniq.filter(_.key == k))
    val filtered = forKey.filter(_ > query.offsetTag)
    val sorted = filtered.sortWith(_ < _)

    sorted.take(query.limit)
  }

  def findKeys(query: TagQuery): List[TagKey] = {
    findValues(query).map(v => TagKey(v))
  }

  def findValues(query: TagQuery): List[String] = {
    val matches = findItemsImpl(query.query)
    val uniq = matches.flatMap(_.tags).distinct

    val forKey = query.key.fold(uniq.map(_._2))(k => uniq.filter(_._1 == k).map(_._1))
    val filtered = forKey.filter(_ > query.offset)
    val sorted = filtered.sortWith(_ < _)

    sorted.take(query.limit)
  }

  def findItems(query: TagQuery): List[T] = {
    val matches = findItemsImpl(query.query)
    val filtered = matches.filter(i => "%040x".format(i.id) > query.offset)
    val sorted = filtered.sortWith { (a, b) => a.id.compareTo(b.id) < 0 }
    sorted.take(query.limit)
  }

  val size: Int = items.length

  def update(additions: List[T], deletions: List[BigInteger]): TagIndex[T] = {
    val itemMap = items.map(i => i.id -> i).toMap -- deletions
    val finalMap = itemMap ++ additions.map(i => i.id -> i)
    val newItems = finalMap.values.toList.sortWith((a, b) => a.id.compareTo(b.id) < 0)
    new SimpleTagIndex(newItems.toArray)
  }
}
