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
package org.apache.lens.cube.metadata;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.lens.api.LensException;
import org.apache.lens.cube.parse.TimeRange;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.Table;

import lombok.Data;

@Data
public class PartitionTimeline {
  private TimePartition first;
  private TreeSet<TimePartition> holes = new TreeSet<TimePartition>();
  private TimePartition latest;
  // Temporary. Kind of Internal use
  private TreeSet<TimePartition> all;

  public void addPartition(UpdatePeriod updatePeriod, String value) throws LensException {
    addPartition(new TimePartition(updatePeriod, value));
  }

  public void addPartition(TimePartition partition) throws LensException {
    if (isEmpty()) {
      // First partition being added
      first = partition;
      latest = partition;
      return;
    }
    if (partition.before(first)) {
      addHolesBetween(partition, first, partition.getUpdatePeriod());
      first = partition;
    } else if (partition.after(latest)) {
      addHolesBetween(latest, partition, partition.getUpdatePeriod());
      latest = partition;
    } else {
      holes.remove(partition);
    }
  }

  public void dropPartition(UpdatePeriod updatePeriod, String value, boolean isExists) throws LensException {
    if (isEmpty()) {
      throw new RuntimeException("Deleting a partition whose partitions are uninitialized in cache");
    }

    if (isExists) {
      // nothing to do, the same date time value exists, so no changes to cached values
      return;
    }
    TimePartition toDrop = new TimePartition(updatePeriod, value);
    if (first.equals(latest) && first.equals(toDrop)) {
      this.first = null;
      this.latest = null;
      this.holes.clear();
    } else if (first.equals(toDrop)) {
      this.first = this.getNextPartition(first, latest, updatePeriod, 1);
    } else if (latest.equals(toDrop)) {
      this.latest = this.getNextPartition(latest, first, updatePeriod, -1);
    } else {
      this.addHole(toDrop);
    }
  }

  private void addHole(TimePartition toDrop) {
    //TODO: improve performance by taking both Date and String as argument. Parsing/formatting overhead will be gone
    holes.add(toDrop);
  }

  private void addHolesBetween(TimePartition begin, TimePartition end, UpdatePeriod updatePeriod) throws LensException {
    for (Date date : TimeRange.iterable(begin.next().getDate(), end.getDate(), updatePeriod, 1)) {
      addHole(new TimePartition(updatePeriod, date));
    }
  }

  private TimePartition getNextPartition(TimePartition begin, TimePartition end, UpdatePeriod updatePeriod,
    int increment) throws LensException {
    for (Date date : TimeRange.iterable(begin.partitionAtDiff(increment).getDate(),
      end.partitionAtDiff(increment).getDate(), updatePeriod, increment)) {
      TimePartition value = new TimePartition(updatePeriod, date);
      if (!holes.contains(value)) {
        return value;
      } else {
        holes.remove(value);
      }
    }
    return null;
  }

  public void reduce() throws LensException {
    //TODO: improve algo by adding bulk addition.
    for (TimePartition part : getAll()) {
      addPartition(part);
    }
    // hint GC
    all = null;
  }

  public Map<? extends String, ? extends String> toProperties(UpdatePeriod updatePeriod, String partCol) {
    Map<String, String> params = new HashMap<String, String>();
    if (isEmpty()) {
      return params;
    }
    params.put(MetastoreUtil.getPartitionInfoKeyForFirst(updatePeriod, partCol), getFirst().getDateString());
    params.put(MetastoreUtil.getPartitionInfoKeyForLatest(updatePeriod, partCol), getLatest().getDateString());
    params.put(MetastoreUtil.getPartitionInfoKeyForHoles(updatePeriod, partCol), StringUtils.join(getHoles(), ","));
    return params;
  }

  public boolean isEmpty() {
    return first == null && latest == null && holes.isEmpty();
  }

  public boolean exists(UpdatePeriod updatePeriod, Date partSpec) throws LensException {
    if (isEmpty()) {
      return false;
    }
    TimePartition toCheck = new TimePartition(updatePeriod, partSpec);
    return !toCheck.before(first) && !toCheck.after(latest) && !holes.contains(toCheck);
  }

  public static PartitionTimeline readFromTable(
    Table storageTable, UpdatePeriod updatePeriod, String partCol) throws LensException {
    PartitionTimeline timeline = new PartitionTimeline();
    String first = storageTable.getParameters().get(MetastoreUtil.getPartitionInfoKeyForFirst(updatePeriod, partCol));
    String latest = storageTable.getParameters().get(MetastoreUtil.getPartitionInfoKeyForLatest(updatePeriod, partCol));
    String holes = storageTable.getParameters().get(MetastoreUtil.getPartitionInfoKeyForHoles(updatePeriod, partCol));
    timeline.setFirst(new TimePartition(updatePeriod, first));
    timeline.setLatest(new TimePartition(updatePeriod, latest));
    timeline.getHoles().clear();
    if (holes != null) {
      for (String s : holes.split("\\s*,\\s*")) {
        timeline.getHoles().add(new TimePartition(updatePeriod, s));
      }
    }
    return timeline;
  }

  public Date getLatestDate() {
    return getLatest() == null ? null : getLatest().getDate();
  }
}
