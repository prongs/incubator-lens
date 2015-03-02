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
package org.apache.lens.cube.parse;

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.apache.lens.cube.metadata.UpdatePeriod;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import lombok.Getter;

/**
 * Timerange data structure
 */
public class TimeRange {
  private String partitionColumn;
  private Date toDate;
  private Date fromDate;
  private ASTNode astNode;
  private ASTNode parent;
  private int childIndex;

  public static class TimeRangeBuilder {
    private final TimeRange range;

    public TimeRangeBuilder() {
      this.range = new TimeRange();
    }

    public TimeRangeBuilder partitionColumn(String col) {
      range.partitionColumn = col;
      return this;
    }

    public TimeRangeBuilder toDate(Date to) {
      range.toDate = to;
      return this;
    }

    public TimeRangeBuilder fromDate(Date from) {
      range.fromDate = from;
      return this;
    }

    public TimeRangeBuilder astNode(ASTNode node) {
      range.astNode = node;
      return this;
    }

    public TimeRangeBuilder parent(ASTNode parent) {
      range.parent = parent;
      return this;
    }

    public TimeRangeBuilder childIndex(int childIndex) {
      range.childIndex = childIndex;
      return this;
    }

    public TimeRange build() {
      return range;
    }
  }

  public static TimeRangeBuilder getBuilder() {
    return new TimeRangeBuilder();
  }

  private TimeRange() {

  }

  public String getPartitionColumn() {
    return partitionColumn;
  }

  public Date getFromDate() {
    return fromDate;
  }

  public Date getToDate() {
    return toDate;
  }

  public ASTNode getASTNode() {
    return astNode;
  }

  public ASTNode getParent() {
    return parent;
  }

  public int getChildIndex() {
    return childIndex;
  }

  public void validate() throws SemanticException {
    if (partitionColumn == null || fromDate == null || toDate == null || fromDate.equals(toDate)) {
      throw new SemanticException(ErrorMsg.INVALID_TIME_RANGE);
    }

    if (fromDate.after(toDate)) {
      throw new SemanticException(ErrorMsg.FROM_AFTER_TO, fromDate.toString(), toDate.toString());
    }
  }

  @Override
  public String toString() {
    return partitionColumn + " [" + fromDate + ":" + toDate + "]";
  }

  public static CalendarIterable iterable(Date fromDate, Date toDate, UpdatePeriod updatePeriod, int increment) {
    return TimeRange.getBuilder().fromDate(fromDate).toDate(toDate).build().iterable(updatePeriod, increment);
  }

  public static CalendarIterable iterable(Date fromDate, int numIters, UpdatePeriod updatePeriod, int increment) {
    return TimeRange.getBuilder().fromDate(fromDate).build().iterable(updatePeriod, numIters, increment);
  }

  public static CalendarIterable iterable(Date fromDate, int numIters, UpdatePeriod updatePeriod) {
    //TODO: define a constant DEFAULT_INCREMENT = 1
    return TimeRange.getBuilder().fromDate(fromDate).build().iterable(updatePeriod, numIters, 1);
  }

  private CalendarIterable iterable(UpdatePeriod updatePeriod, int numIters, int increment) {
    return new CalendarIterable(updatePeriod, numIters, increment);
  }

  public static CalendarIterable iterable(Date fromDate, Date toDate, UpdatePeriod updatePeriod) {
    return TimeRange.getBuilder().fromDate(fromDate).toDate(toDate).build().iterable(updatePeriod);
  }


  public CalendarIterable iterable(UpdatePeriod updatePeriod) {
    return iterable(updatePeriod, 1);
  }

  public CalendarIterable iterable(UpdatePeriod updatePeriod, int increment) {
    return new CalendarIterable(updatePeriod, increment);
  }

  public class CalendarIterable implements java.lang.Iterable<Date> {
    private UpdatePeriod updatePeriod;
    private long numIters;
    private int increment;

    public CalendarIterable(UpdatePeriod updatePeriod, int increment) {
      this(updatePeriod, DateUtil.getTimeDiff(fromDate, toDate, updatePeriod) / increment, increment);
    }

    public CalendarIterable(UpdatePeriod updatePeriod, long numIters, int increment) {
      this.updatePeriod = updatePeriod;
      this.numIters = numIters;
      if (numIters < 0) {
        numIters = 0;
      }
      this.increment = increment;
    }

    @Override
    public CalendarIterator iterator() {
      return new CalendarIterator();
    }

    public class CalendarIterator implements Iterator<Date> {
      Calendar calendar;
      // Tracks the index of the item returned after the last next() call.
      // Index here refers to the index if the iterator were iterated and converted into a list.
      @Getter
      int counter = -1;

      {
        calendar = Calendar.getInstance();
        calendar.setTime(fromDate);
        if (toDate != null) {
          numIters = DateUtil.getTimeDiff(fromDate, toDate, updatePeriod);
        } else {
          numIters = CalendarIterable.this.numIters;
        }
      }

      @Override
      public boolean hasNext() {
        return counter < numIters - 1;
      }

      @Override
      public Date next() {
        Date cur = calendar.getTime();
        calendar.add(updatePeriod.calendarField(), increment);
        counter++;
        return cur;
      }

      public Date peekNext() {
        return calendar.getTime();
      }

      @Override
      public void remove() {
        throw new RuntimeException("Not allowed");
      }

      public long getNumIters() {
        return numIters;
      }
    }
  }
}
