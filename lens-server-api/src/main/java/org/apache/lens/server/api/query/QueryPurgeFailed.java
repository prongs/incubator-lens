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
package org.apache.lens.server.api.query;

import lombok.Getter;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.UUID;

/**
 * Event fired when a query fails to purge. Use getCause() to get the reason for failure.
 */
public class QueryPurgeFailed extends QueryEvent<String> {
  protected final UUID id = UUID.randomUUID();
  /**
   * query context of the failed query
   */
  @Getter
  private final QueryContext context;
  /**
   * cause of purge failure. Can be used in handler
   */
  @Getter
  private final Exception cause;
  /**
   * Number of failed tries.
   */
  @Getter
  private final int numTries;
  private final HiveConf serverConf;

  public QueryPurgeFailed(QueryContext context, HiveConf serverConf, int numTries, Exception e) {
    super(System.currentTimeMillis(), context.getUserQuery(), context.getUserQuery(), context.getQueryHandle());
    this.context = context;
    this.serverConf = serverConf;
    this.cause = e;
    this.numTries = numTries;
  }
}
