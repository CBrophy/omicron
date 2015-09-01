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


import java.time.Duration;
import java.time.LocalTime;
import java.time.ZonedDateTime;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A class to represent a time interval
 */
public class TimeInterval {
  private final LocalTime startTime;
  private final Duration hours;

  /**
   * Constructor
   *
   * @param startTime The time to start the interval (inclusive)
   * @param hours     The number of hours beyond the start time to include in the interval
   */
  TimeInterval(final LocalTime startTime, final int hours) {
    checkNotNull(startTime, "startTime");
    checkArgument(hours > 0, "hours must be positive");
    this.hours = Duration.ofHours(hours);
    this.startTime = startTime;
  }

  /**
   * Tests to determine if an zonedDateTime is contained in the time interval (inclusive)
   * @param zonedDateTime The zonedDateTime to test
   * @return True if zonedDateTime is within the range of the time interval
   */
  public boolean contains(final ZonedDateTime zonedDateTime){
    checkNotNull(zonedDateTime, "zonedDateTime");

    final ZonedDateTime startInstant = zonedDateTime.with(startTime);
    final ZonedDateTime endInstant = startInstant.plus(hours);

    return (startInstant.isBefore(zonedDateTime) || startInstant.equals(zonedDateTime)) && (endInstant.isAfter(zonedDateTime) || endInstant.equals(zonedDateTime));
  }
}
