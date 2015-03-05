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


import java.util.*;

import org.apache.lens.api.LensException;
import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.metadata.MetastoreUtil;
import org.apache.lens.cube.metadata.TimePartition;
import org.apache.lens.cube.metadata.UpdatePeriod;

import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Data;
import lombok.NonNull;

/**
 * Represents the in-memory data structure that represents timeline of all existing partitions for a given storage
 * table, update period, partition column. Is an Abstract class. Can be implemented in multiple ways.
 *
 * @see org.apache.lens.cube.metadata.timeline.EndsAndHolesPartitionTimeline
 */
@Data
public abstract class PartitionTimeline {
  /**
   * Add partition to local memory to be sent for batch addigion
   * @see com
   */
  public void addForBatchAddition(TimePartition partition) {
    if (all == null) {
      all = Sets.newTreeSet();
    }
    all.add(partition);
  }

  private final CubeMetastoreClient client;
  private final String storageTableName;
  private final UpdatePeriod updatePeriod;
  private final String partCol;
  private TreeSet<TimePartition> all;

  public boolean add(UpdatePeriod updatePeriod, String value) throws LensException {
    return add(TimePartition.of(updatePeriod, value));
  }

  public Date getLatestDate() {
    return latest() == null ? null : latest().getDate();
  }

  public void updateTableParams(Table table) {
    String prefix = MetastoreUtil.getPartitionInfoKeyPrefix(getUpdatePeriod(), getPartCol());
    String storageClass = MetastoreUtil.getPartitionTimelineStorageClassKey(getUpdatePeriod(), getPartCol());
    table.getParameters().put(storageClass, this.getClass().getCanonicalName());
    for (Map.Entry<String, String> entry : toProperties().entrySet()) {
      table.getParameters().put(prefix + entry
        .getKey(), entry.getValue());
    }
  }

  public void init(Table table) throws LensException {
    HashMap<String, String> props = Maps.newHashMap();
    String prefix = MetastoreUtil.getPartitionInfoKeyPrefix(getUpdatePeriod(), getPartCol());
    for (Map.Entry<String, String> entry : table.getParameters().entrySet()) {
      if (entry.getKey().startsWith(prefix)) {
        props.put(entry.getKey().substring(prefix.length()), entry.getValue());
      }
    }
    initFromProperties(props);
  }

  public boolean commitBatchAdditions() throws LensException {
    if (getAll() == null) {
      return true;
    }
    boolean result = add(getAll());
    all = null;
    return result;
  }

  public abstract boolean add(@NonNull TimePartition partition) throws LensException;

  public abstract boolean add(@NonNull Collection<TimePartition> partition) throws LensException;

  public abstract boolean drop(@NonNull TimePartition toDrop) throws LensException;

  public abstract TimePartition latest();

  public abstract Map<String, String> toProperties();

  public abstract boolean initFromProperties(Map<String, String> properties) throws LensException;

  public abstract boolean isEmpty();

  public abstract boolean isConsistent();

  public abstract boolean exists(TimePartition partition);
}
