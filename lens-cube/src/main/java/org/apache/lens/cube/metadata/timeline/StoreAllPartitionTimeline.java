/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.cube.metadata.timeline;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.lens.api.LensException;
import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.metadata.TimePartition;
import org.apache.lens.cube.metadata.UpdatePeriod;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.NonNull;

/**
 * Implementation of PartitionTimeline that stores all partitions as a tree set.
 */
public class StoreAllPartitionTimeline extends PartitionTimeline {
  TreeSet<TimePartition> allPartitions;

  public StoreAllPartitionTimeline(CubeMetastoreClient client, String storageTableName,
    UpdatePeriod updatePeriod, String partCol) {
    super(client, storageTableName, updatePeriod, partCol);
    allPartitions = Sets.newTreeSet();
  }

  @Override
  public boolean add(@NonNull TimePartition partition) throws LensException {
    return allPartitions.add(partition);
  }

  @Override
  public boolean add(@NonNull Collection<TimePartition> partitions) throws LensException {
    return allPartitions.addAll(partitions);
  }

  @Override
  public boolean drop(@NonNull TimePartition toDrop) throws LensException {
    if (morePartitionsExist(toDrop.getDateString())) {
      return true;
    }
    return allPartitions.remove(toDrop);
  }

  @Override
  public TimePartition latest() {
    return allPartitions.size() == 0 ? null : allPartitions.last();
  }

  @Override
  public Map<String, String> toProperties() {
    HashMap<String, String> map = Maps.newHashMap();
    map.put("partitions", StringUtils.join(allPartitions, ","));
    return map;
  }

  @Override
  public boolean initFromProperties(Map<String, String> properties) throws LensException {
    String partitionsStr = properties.get("partitions");
    if (partitionsStr == null) {
      return true;
    }
    boolean ret = true;
    for (String s : StringUtils.split(partitionsStr, ",")) {
      ret &= add(TimePartition.of(getUpdatePeriod(), s));
    }
    return ret;
  }

  @Override
  public boolean isEmpty() {
    return allPartitions.isEmpty();
  }

  @Override
  public boolean isConsistent() {
    return true;
  }

  @Override
  public boolean exists(TimePartition partition) {
    return allPartitions.contains(partition);
  }
}
