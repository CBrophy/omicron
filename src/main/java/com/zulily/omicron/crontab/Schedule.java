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
import org.joda.time.LocalDateTime;

import static com.google.common.base.Preconditions.checkNotNull;

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

  public boolean timeInSchedule(final LocalDateTime localDateTime) {
    checkNotNull(localDateTime, "localDateTime");

    return daysOfWeek.contains(extractDayOfWeek(localDateTime))
      && months.contains(localDateTime.getMonthOfYear())
      && days.contains(localDateTime.getDayOfMonth())
      && hours.contains(localDateTime.getHourOfDay())
      && minutes.contains(localDateTime.getMinuteOfHour());
  }

  public LocalDateTime getNextRunAfter(final LocalDateTime localDateTime) {
    checkNotNull(localDateTime, "localDateTime");

    int hour  = localDateTime.getHourOfDay();
    int minute = localDateTime.getMinuteOfHour();

    // It's never the current minute, always the next or first available
    // minute in the following hour
    Integer nextMinute = minutes.higher(minute);

    if (nextMinute == null) {
      nextMinute = minutes.first();
    }

    // If the minute flipped over, then its the next highest hour
    // or the first hour if no next highest hour exists

    Integer nextHour = hours.ceiling(nextMinute > minute ? hour : hour + 1);

    if (nextHour == null) {
      nextHour = hours.first();
    }

    LocalDateTime result = localDateTime.withMinuteOfHour(nextMinute).withHourOfDay(nextHour);

    // If the hour rolled over to the next day, or the current day is not in schedule
    // just get the next calendar day in schedule at the start of the permitted minute & hour

    if (nextHour < hour || !timeInSchedule(result)) {
      return findRunDayCeiling(result.plusDays(1));
    } else {
      return result;
    }

  }

  private LocalDateTime findRunDayCeiling(final LocalDateTime localDateTime){
    LocalDateTime result = new LocalDateTime(localDateTime).withMinuteOfHour(minutes.first()).withHourOfDay(hours.first());


    // Looping is simpler, albeit not efficient compared
    // to the logic of reconciling day, month, and dayOfWeek
    // with the possible conflict between day/month
    // as they may or may not be an appropriate day of week to
    // meet schedule criteria

    while (!timeInSchedule(result)) {
      result = result.plusDays(1);
    }

    return result;
  }

  private static int extractDayOfWeek(final LocalDateTime localDateTime) {
    // joda-time uses 1-7 dayOfWeek with Sunday as 7, so convert 7 to 0 to match crontab expression range of 0-6
    // see evaluateExpressionPart() comments for more information

    return localDateTime.getDayOfWeek() == 7 ? 0 : localDateTime.getDayOfWeek();
  }
}
