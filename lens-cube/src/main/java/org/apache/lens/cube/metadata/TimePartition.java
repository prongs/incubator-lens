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
import java.util.Calendar;
import java.util.Date;

import org.apache.lens.api.LensException;

import lombok.Data;

@Data
public class TimePartition implements Comparable<TimePartition> {

  private final UpdatePeriod updatePeriod;
  private final Date date;
  private final String dateString;

  public TimePartition(UpdatePeriod updatePeriod, Date date) throws LensException {
    // Ensure date is truncated
    this(updatePeriod, updatePeriod.format().format(date));
  }

  public TimePartition(UpdatePeriod updatePeriod, String dateString) throws LensException {
    this.updatePeriod = updatePeriod;
    this.dateString = dateString;
    try {
      this.date = updatePeriod.format().parse(dateString);
    } catch (ParseException e) {
      throw new LensException(e);
    }
  }

  public TimePartition(UpdatePeriod updatePeriod, Date date, String dateString) throws LensException {
    this(updatePeriod, date);
    if (!this.updatePeriod.equals(dateString)) {
      throw new LensException("Date: " + date + " is not equal to dateString: " + dateString);
    }
  }

  public String toString() {
    return dateString;
  }

  @Override
  public int compareTo(TimePartition o) {
    if (o == null) {
      return 1;
    }
    return this.date.compareTo(o.date);
  }

  public TimePartition partitionAtDiff(int increment) throws LensException {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(updatePeriod.calendarField(), increment);
    return new TimePartition(updatePeriod, cal.getTime());
  }

  public TimePartition previous() throws LensException {
    return partitionAtDiff(-1);
  }

  public TimePartition next() throws LensException {
    return partitionAtDiff(1);
  }

  public boolean before(TimePartition when) {
    return this.date.before(when.date);
  }

  public boolean after(TimePartition when) {
    return this.date.after(when.date);
  }
}
