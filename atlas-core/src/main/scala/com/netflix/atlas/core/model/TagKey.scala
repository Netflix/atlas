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
package com.netflix.atlas.core.model

/** Helper functions and constants for standard tag keys. */
object TagKey {

  private final val atlasPrefix = "atlas."
  private final val netflixPrefix = "nf."

  /**
   * Synthetic tag that is created for lines in a graph to indicate all key/value pairs for
   * a particular line on the graph.
   */
  final val allTags = atlasPrefix + "allTags"

  /**
   * Synthetic tag that is created for lines in a graph to indicate the unique key/value pairs for
   * an aggregate line.
   */
  final val uniqueTags = atlasPrefix + "uniqueTags"

  /**
   * Synthetic tag that is created for lines in a graph to indicate the time offset the line is
   * shifted by.
   */
  final val offset = atlasPrefix + "offset"

  /** Data source type for the metric. */
  final val dsType = atlasPrefix + "dstype"

  /** Indicates the legacy system the metric came from such as epic. */
  final val legacy = atlasPrefix + "legacy"

  /** Indicates whether the data was uploaded with the batch or real-time policy. */
  final val policy = atlasPrefix + "policy"

  /** Indicates the source of a metric such as plugin or snmp. */
  final val source = atlasPrefix + "source"

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

  /** ISO country code. */
  final val country = netflixPrefix + "country"

  /** ISO country code rollup used by NCCP. */
  final val countryRollup = netflixPrefix + "country.rollup"

  /** List of atlas tag keys. */
  val atlas: List[String] = List(
    allTags, uniqueTags,
    dsType, legacy, policy, source)

  /** List of standard netflix tag keys. */
  val netflix: List[String] = List(
    application, cluster, autoScalingGroup, node,
    region, availabilityZone,
    vmType, image,
    country, countryRollup)

  /** A list of standard tags. */
  val standard: List[String] = atlas ::: netflix

  /**
   * TagKey names with these prefixes are restricted and managed by the system. They should not be
   * set or used directly by users.
   */
  val restrictedPrefixes: List[String] = List(atlasPrefix, netflixPrefix)

  private val standardTagKeys = standard.toSet

  /** Returns true if the name uses a restricted prefix. */
  def isRestricted(name: String): Boolean = restrictedPrefixes.exists(name.startsWith)

  /** Returns true if the name is one of the standard tag keys. */
  def isStandard(name: String): Boolean = standardTagKeys.contains(name)

  /**
   * Returns true if the name is valid. A name is invalid if it is a non-standard name using a
   * restricted prefix.
   */
  def isValid(name: String): Boolean = !isRestricted(name) || isStandard(name)

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

