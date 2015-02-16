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

import java.util.*;

import lombok.Data;
import lombok.NoArgsConstructor;
//Stored inside CubeFactTable
public class PartitionInfo extends HashMap<String, //storage
  TreeMap<UpdatePeriod,
    Map<String, // partition column
      PartitionInfo.PartitionTimeline>>> {
  @Data
  public static class PartitionTimeline {
    private String first;
    private String latest;
    private List<String> holes = new ArrayList<String>();
  }
  public Map<String, PartitionTimeline> get(String storageTable, UpdatePeriod period) {
    return get(storageTable).get(period);
  }
  public PartitionTimeline get(String storageTable, UpdatePeriod period, String partCol) {
    return get(storageTable).get(period).get(partCol);
  }
}
