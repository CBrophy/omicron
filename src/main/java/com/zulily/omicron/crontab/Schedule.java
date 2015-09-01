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
package com.zulily.omicron.crontab;

import com.google.common.collect.ImmutableSortedSet;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Encapsulates the cron schedule whitelists and any related scheduling logic
 */
public class Schedule {
  private final ImmutableSortedSet<Integer> minutes;
  private final ImmutableSortedSet<Integer> hours;
  private final ImmutableSortedSet<Integer> days;
  private final ImmutableSortedSet<Integer> months;
  private final ImmutableSortedSet<Integer> daysOfWeek;

  Schedule(
    final ImmutableSortedSet<Integer> minutes,
    final ImmutableSortedSet<Integer> hours,
    final ImmutableSortedSet<Integer> days,
    final ImmutableSortedSet<Integer> months,
    final ImmutableSortedSet<Integer> daysOfWeek
  ) {
    this.minutes = minutes;
    this.hours = hours;
    this.days = days;
    this.months = months;
    this.daysOfWeek = daysOfWeek;
  }

  ImmutableSortedSet<Integer> getMinutes() {
    return minutes;
  }

  ImmutableSortedSet<Integer> getHours() {
    return hours;
  }

  ImmutableSortedSet<Integer> getDays() {
    return days;
  }

  ImmutableSortedSet<Integer> getMonths() {
    return months;
  }

  ImmutableSortedSet<Integer> getDaysOfWeek() {
    return daysOfWeek;
  }

  /**
   * Determines whether or not a zonedDateTime is whitelisted by the defined schedule
   *
   * @param zonedDateTime The date and time to test
   * @return True if the time is in schedule, False otherwise
   */
  public boolean timeInSchedule(final ZonedDateTime zonedDateTime) {
    checkNotNull(zonedDateTime, "zonedDateTime");

    return daysOfWeek.contains(convertToCronDayOfWeek(zonedDateTime.get(ChronoField.DAY_OF_WEEK)))
      && months.contains(zonedDateTime.get(ChronoField.MONTH_OF_YEAR))
      && days.contains(zonedDateTime.get(ChronoField.DAY_OF_MONTH))
      && hours.contains(zonedDateTime.get(ChronoField.HOUR_OF_DAY))
      && minutes.contains(zonedDateTime.get(ChronoField.MINUTE_OF_HOUR));
  }

  private static int convertToCronDayOfWeek(final int dayOfWeek) {
    // joda-time uses 1-7 dayOfWeek with Sunday as 7, so convert 7 to 0 to match crontab expression range of 0-6
    // see evaluateExpressionPart() comments for more information

    return dayOfWeek == 7 ? 0 : dayOfWeek;
  }
}
