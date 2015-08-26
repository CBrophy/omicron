/*
 * Copyright (C) 2014 zulily, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zulily.omicron.conf;

import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.LocalTime;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A class to represent a time interval
 */
public class TimeInterval {
  private final LocalTime startTime;
  private final int hours;

  /**
   * Constructor
   *
   * @param startTime The time to start the interval (inclusive)
   * @param hours     The number of hours beyond the start time to include in the interval
   */
  TimeInterval(final LocalTime startTime, final int hours) {
    checkNotNull(startTime, "startTime");
    checkArgument(hours > 0, "hours must be positive");
    this.hours = hours;
    this.startTime = startTime;
  }

  public LocalTime getStartTime() {
    return startTime;
  }

  public int getHours() {
    return hours;
  }

  /**
   * Get this object as an Interval instance
   *
   * @param chronology The chronology of this interval's local time object
   * @return The interval object represented by startTime + hours
   */
  public Interval asInterval(final Chronology chronology) {
    checkNotNull(chronology, "chronology");

    DateTime start = DateTime.now(chronology).withTime(startTime.getHourOfDay(), startTime.getMinuteOfHour(), 0, 0);
    DateTime end = start.plusHours(hours);

    return new Interval(start, end);
  }
}
