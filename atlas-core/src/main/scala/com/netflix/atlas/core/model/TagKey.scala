/*
 * Copyright 2014-2022 Netflix, Inc.
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
package com.netflix.atlas.core.model

/** Helper functions and constants for standard tag keys. */
object TagKey {

  private final val atlasPrefix = "atlas."
  private final val netflixPrefix = "nf."

  /**
    * Synthetic tag that is created for lines in a graph to indicate the time offset the line is
    * shifted by.
    */
  final val offset = atlasPrefix + "offset"

  /**
    * Synthetic tag that is created for lines in a graph to substitute the average statistic
    * for the line.
    */
  final val avg = atlasPrefix + "avg"

  /**
    * Synthetic tag that is created for lines in a graph to substitute the max statistic
    * for the line.
    */
  final val max = atlasPrefix + "max"

  /**
    * Synthetic tag that is created for lines in a graph to substitute the min statistic
    * for the line.
    */
  final val min = atlasPrefix + "min"

  /**
    * Synthetic tag that is created for lines in a graph to substitute the last statistic
    * for the line.
    */
  final val last = atlasPrefix + "last"

  /**
    * Synthetic tag that is created for lines in a graph to substitute the total statistic
    * for the line.
    */
  final val total = atlasPrefix + "total"

  /** Data source type for the metric. */
  final val dsType = atlasPrefix + "dstype"

  /** Indicates the legacy system the metric came from such as epic. */
  final val legacy = atlasPrefix + "legacy"

  /** Application associated with the metric. */
  final val application = netflixPrefix + "app"

  /** Cluster name associated with the metric. */
  final val cluster = netflixPrefix + "cluster"

  /** Amazon auto scaling group for the instance reporting the metric. */
  final val autoScalingGroup = netflixPrefix + "asg"

  /** Instance id reporting the metric. */
  final val node = netflixPrefix + "node"

  /** Amazon region for the instance reporting the metric. */
  final val region = netflixPrefix + "region"

  /** Amazon availability zone for the instance reporting the metric. */
  final val availabilityZone = netflixPrefix + "zone"

  /** Amazon instance type for the instance reporting the metric. */
  final val vmType = netflixPrefix + "vmtype"

  /** Image id for the instance reporting the metric. */
  final val image = netflixPrefix + "ami"

  /** Used to store the bucket id for percentile approximation. */
  final val percentile = "percentile"

  /**
    * Returns true if the name uses a restricted prefix that is managed by the system. These
    * tag keys should not be set or used directly by users
    */
  def isRestricted(name: String): Boolean = {
    name.startsWith(netflixPrefix) || name.startsWith(atlasPrefix)
  }

  /** Creates a new tag key object with unknown count. */
  def apply(name: String): TagKey = TagKey(name, -1)
}

/**
  * Key name and an associated count.
  *
  * @param name   key for the tag
  * @param count  number of items with this tag key or -1 if unknown
  */
case class TagKey(name: String, count: Int) {

  def <(t: TagKey): Boolean = compareTo(t) < 0

  def >(t: TagKey): Boolean = compareTo(t) > 0

  /** Tags are ordered by key, value, and then count. */
  def compareTo(t: TagKey): Int = {
    val k = name.compareTo(t.name)
    if (k != 0) k else count - t.count
  }
}
