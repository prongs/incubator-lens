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

import java.text.ParseException;
import java.util.*;

import lombok.Data;

//Stored inside CubeFactTable
public class PartitionInfo extends HashMap<String, //storage table
  TreeMap<UpdatePeriod,
    Map<String, // partition column
      PartitionInfo.PartitionTimeline>>> {


  @Data
  public static class PartitionTimeline {
    private String first;
    private String latest;
    private List<String> holes = new ArrayList<String>();
    // Temporary. Kind of Internal use
    private TreeSet<Date> all;

    public void addPartition(UpdatePeriod updatePeriod, String value) {
      if(isUninitialized()) {
        // First partition being added
        first = value;
        latest = value;
        return;
      }
      try {
        Date firstDate = updatePeriod.format().parse(first);
        Date latestDate = updatePeriod.format().parse(latest);
        Date toAddDate = updatePeriod.format().parse(value);
        if (toAddDate.before(firstDate)) {
          first = updatePeriod.format().format(toAddDate);
          addHolesBetween(firstDate, toAddDate, updatePeriod, true);
        } else if (toAddDate.after(latestDate)) {
          latest = updatePeriod.format().format(toAddDate);
          addHolesBetween(latestDate, toAddDate, updatePeriod, false);
        } else {
          holes.remove(value);
        }
      } catch (ParseException e) {
        throw new RuntimeException("Shouldn't happen");
      }
    }

    private void addHolesBetween(Date begin, Date end, UpdatePeriod updatePeriod, boolean negative) {
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(begin);
      calendar.add(updatePeriod.calendarField(), negative ? -1 : 1);

      while (negative ? calendar.getTime().after(end) : calendar.getTime().before(end)) {
        String holeStr = updatePeriod.format().format(calendar.getTime());
        if (negative) {
          holes.add(0, holeStr);
        } else {
          holes.add(holeStr);
        }
        calendar.add(updatePeriod.calendarField(), negative ? -1 : 1);
      }
    }

    public void reduce(UpdatePeriod updatePeriod) {
      //TODO: improve algo by adding bulk addition.
      for(Date partDate: getAll()) {
        addPartition(updatePeriod, updatePeriod.format().format(partDate));
      }
      //TODO: all = null;
    }

    public Map<? extends String, ? extends String> toProperties(UpdatePeriod updatePeriod, String partCol) {
      Map<String, String> params = new HashMap<String, String>();
      if(isUninitialized()) {
        return params;
      }
      params.put(MetastoreUtil.getPartitionInfoKeyForFirst(updatePeriod, partCol), getFirst());
      params.put(MetastoreUtil.getPartitionInfoKeyForLatest(updatePeriod, partCol), getLatest());
      StringBuilder holesStringBuilder = new StringBuilder();
      String sep = "";
      for(String s: getHoles()) {
        holesStringBuilder.append(sep).append(s);
        sep = ",";
      }
      params.put(MetastoreUtil.getPartitionInfoKeyForHoles(updatePeriod, partCol), holesStringBuilder.toString());
      return params;
    }

    public boolean isUninitialized() {
      return first == null && latest == null && holes.isEmpty() ;
    }
  }

  public TreeMap<UpdatePeriod, Map<String, PartitionTimeline>> get(String fact, String storage) {
    return get(MetastoreUtil.getStorageTableName(fact, Storage.getPrefix(storage)));
  }

  public void registerPartitionExistance(String storageTable, UpdatePeriod updatePeriod, String partitionColumn,
    String partition) {
    ensureEntry(storageTable, updatePeriod, partitionColumn);
    if (get(storageTable).get(updatePeriod).get(partitionColumn).getAll() == null) {
      get(storageTable).get(updatePeriod).get(partitionColumn).setAll(new TreeSet<Date>());
    }
    try {
      get(storageTable).get(updatePeriod).get(partitionColumn).getAll().add(updatePeriod.format().parse(partition));
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }
  public void ensureEntry(String storageTable, UpdatePeriod updatePeriod, String partitionColumn) {
    if (get(storageTable) == null) {
      put(storageTable, new TreeMap<UpdatePeriod, Map<String, PartitionTimeline>>());
    }
    if (get(storageTable).get(updatePeriod) == null) {
      get(storageTable).put(updatePeriod, new HashMap<String, PartitionTimeline>());
    }
    if (get(storageTable).get(updatePeriod).get(partitionColumn) == null) {
      get(storageTable).get(updatePeriod).put(partitionColumn, new PartitionTimeline());
    }
  }


  public void reduce(String storageTable) {
    for (UpdatePeriod updatePeriod : get(storageTable).keySet()) {
      for (String partCol : get(storageTable).get(updatePeriod).keySet()) {
        PartitionTimeline timeline = get(storageTable).get(updatePeriod).get(partCol);
        Date first = timeline.getAll().first();
        timeline.setFirst(updatePeriod.format().format(first));
        Date latest = timeline.getAll().last();
        timeline.setLatest(updatePeriod.format().format(latest));
        get(storageTable).get(updatePeriod).get(partCol).reduce(updatePeriod);
      }
    }
  }

  public void addPartition(String cubeTableName, String storageName, StoragePartitionDesc partSpec) {
    Map<String, PartitionTimeline> timelines = get(cubeTableName, storageName).get(partSpec.getUpdatePeriod());
    for (Map.Entry<String, String> entry : partSpec.getStoragePartSpec().entrySet()) {
      //Assume timelines has all the time part columns.
      timelines.get(entry.getKey()).addPartition(partSpec.getUpdatePeriod(), entry.getValue());
    }
  }
}
