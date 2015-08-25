package com.zulily.omicron.conf;

import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.LocalTime;

public class TimeInterval {
  private final LocalTime startTime;
  private final int hours;

  TimeInterval(final LocalTime startTime, final int hours) {
    this.hours = hours;
    this.startTime = startTime;
  }

  public LocalTime getStartTime() {
    return startTime;
  }

  public int getHours() {
    return hours;
  }

  public Interval asInterval(final Chronology chronology) {
    DateTime start = DateTime.now(chronology).withTime(startTime.getHourOfDay(), startTime.getMinuteOfHour(), 0, 0);
    DateTime end = start.plusHours(hours);

    return new Interval(start, end);
  }
}
