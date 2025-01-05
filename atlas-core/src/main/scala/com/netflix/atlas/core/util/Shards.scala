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

import java.math.BigInteger

import com.netflix.atlas.core.model.ItemId

/**
  * Utility functions for mapping ids or indices to a shard. For our purposes, a shard
  * is an instance with a set of server groups. The union of data from all groups comprises
  * a full copy of the overall dataset. To allow for smaller deployment units, an individual
  * group or subset of the groups can be replicated. For redundancy, groups could be replicated
  * all the time. At Netflix, we typically replicate the overall set of server groups in
  * another region or zone instead.
  *
  * This class specifically focuses on relatively simple sharding schemes where the component
  * making the decision only needs to know the set of instances and a slot for each instance.
  * Edda is one example of a system that provides this information for AWS auto-scaling
  * groups. More complex sharding schemes that require additional infrastructure, e.g,
  * zookeeper, are out of scope here. There are two sharding modes supported by this
  * class:
  *
  * 1. Mapping an id for a tagged item to a shard. This is typically done while data
  *    is flowing into the system and each datapoint can be routed based on the id.
  *
  * 2. Mapping an positional index to a shard. This is typically done for loading data
  *    that has been processed via Hadoop or similar tools and stored in a fixed number
  *    of files. There should be a manifest with an order list of the files for a given
  *    time and the position can be used to map to a shard. When using this approach it
  *    is recommended to use a [highly composite number][hcn] for the number of files.
  *    This makes it easier to pick a number of groups and sizes for the groups such that
  *    each instance will get the same number of files.
  *
  * When mapping this to AWS an overall deployment is typically a set of auto-scaling groups
  * (ASG). Each instance should get the same amount of data if possible given the set of files.
  * Deployments are typically done as a red/black push of one ASG at a time. So the amount of
  * additional capacity during a push is the size of one of these groups if deployments across
  * the groups are performed serially. While multiple ASGs for a particular group are active
  * the data will be replicated across them.
  *
  * [hcn]: https://en.wikipedia.org/wiki/Highly_composite_number
  */
object Shards {

  /**
    * Creates a mapper used to route data to the appropriate instance. This form is typically
    * used as data is flowing into the system when replicas are not a concern. If replication
    * is needed, then see `replicaMapper` instead.
    *
    * @param group
    *     Single group that makes up the complete data set.
    * @return
    *     Mapper for routing data to instances.
    */
  def mapper[T](group: Group[T]): Mapper[T] = mapper(List(group))

  /**
    * Creates a mapper used to route data to the appropriate instance. This form is typically
    * used as data is flowing into the system when replicas are not a concern. If replication
    * is needed, then see `replicaMapper` instead.
    *
    * @param groups
    *     Set of groups that makes up the complete data set.
    * @return
    *     Mapper for routing data to instances.
    */
  def mapper[T](groups: List[Group[T]]): Mapper[T] = {
    new Mapper[T](groups.sortWith(_.name < _.name).toArray)
  }

  /**
    * Creates a mapper that can be used on an instance of a group. This is typically used
    * if the local instance needs to figure out what data to load.
    *
    * @param groupSize
    *     Size of the group that contains the instance.
    * @param instanceIdx
    *     Index for this instance within the group.
    * @param groupIdx
    *     Index of this group within the overall set of groups.
    * @param numGroups
    *     Number of groups that make up the complete deployment.
    * @return
    *     Mapper
    */
  def localMapper[T](
    groupSize: Int,
    instanceIdx: Int,
    groupIdx: Int,
    numGroups: Int
  ): LocalMapper[T] = {
    new LocalMapper[T](groupSize, instanceIdx, groupIdx, numGroups)
  }

  /**
    * Creates a mapper used to route data to the appropriate instance. This form is used as
    * data is flowing into the system and there can be replicas for the groups.
    *
    * @param groups
    *     Set of groups that makes up the complete deployment.
    * @return
    *     Mapper for routing data to instances.
    */
  def replicaMapper[T](groups: List[Group[T]]): ReplicaMapper[T] = {
    val replicaSets = groups
      .groupBy(_.name)
      .toList
      .sortWith(_._1 < _._1)
      .map(_._2)
      .toArray
    new ReplicaMapper[T](replicaSets)
  }

  /**
    * Group of instances representing a subset of the overall deployment.
    *
    * @param name
    *     Name of the group. In the case of replicas each replica group should have the same
    *     name.
    * @param instances
    *     Instances that are part of the group. The order of this array is important to ensure
    *     that an instance will always get the same set of data. The position is used to associate
    *     data to the instance.
    */
  case class Group[T](name: String, instances: Array[T]) {

    def size: Int = instances.length
  }

  /** Mapper for finding the instance that should receive data for an id or index of a file. */
  class Mapper[T](groups: Array[Group[T]]) {

    require(groups.length > 0, "set of groups cannot be empty")

    /** Return the instance that should receive the data associated with `id`. */
    def instanceForId(id: BigInteger): T = {
      val i = nonNegative(id.intValue())
      instanceForIndex(i)
    }

    /** Return the instance that should receive the data associated with `id`. */
    def instanceForId(id: ItemId): T = {
      val i = nonNegative(id.intValue)
      instanceForIndex(i)
    }

    /** Return the instance that should receive the data for the file with the specified index. */
    def instanceForIndex(i: Int): T = {
      require(i >= 0, "index cannot be negative")
      val group = groups(i % groups.length)
      if (group.size > 0)
        group.instances((i / groups.length) % group.size)
      else
        null.asInstanceOf[T]
    }
  }

  /**
    * Mapper intended to run on a given instance and check to see if data should be loaded
    * there.
    */
  class LocalMapper[T](groupSize: Int, instanceIdx: Int, groupIdx: Int, numGroups: Int) {

    /** Return true if this instance should include data for `id`. */
    def containsId(id: BigInteger): Boolean = {
      val i = nonNegative(id.intValue())
      containsIndex(i)
    }

    /** Return true if this instance should include data for the file with index `i`. */
    def containsIndex(i: Int): Boolean = {
      require(i >= 0, "index cannot be negative")
      (i % numGroups == groupIdx) && ((i / numGroups) % groupSize == instanceIdx)
    }
  }

  /**
    * Mapper for finding the instance that should receive data for an id or index of a file.
    * There can be multiple groups with a given name and data will be replicated across all
    * of the groups for that name.
    */
  class ReplicaMapper[T](groups: Array[List[Group[T]]]) {

    /** Return the instances that should receive the data associated with `id`. */
    def instancesForId(id: BigInteger): List[T] = {
      val i = nonNegative(id.intValue())
      instancesForIndex(i)
    }

    /** Return the instances that should receive the data for the file with the specified index. */
    def instancesForIndex(i: Int): List[T] = {
      require(i >= 0, "index cannot be negative")
      val replicas = groups(i % groups.length)
      replicas.map { group =>
        group.instances((i / groups.length) % group.size)
      }
    }
  }

  /**
    * Returns the absolute value for the number unless it is Integer.MIN_VALUE, in which case
    * it will return 0.
    */
  private[util] def nonNegative(v: Int): Int = {
    math.abs(v) & 0x7FFFFFFF
  }
}
