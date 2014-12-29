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

import com.google.common.base.Joiner;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.zulily.omicron.Utils;
import org.joda.time.LocalDateTime;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Parses a single cron row and returns a numerical schedule of run times
 * for each part of the cron expression.
 * <p/>
 * i.e.
 * 1-10/2 * * * * -> returns 1,3,5,7,9 day vals
 * <p/>
 * # Example of job definition:
 * # .---------------- minute (0 - 59)
 * # |  .------------- hour (0 - 23)
 * # |  |  .---------- day of month (1 - 31)
 * # |  |  |  .------- month (1 - 12) OR jan,feb,mar,apr ...
 * # |  |  |  |  .---- day of week (0 - 7)  (Sunday=0 or 7) OR sun,mon,tue,wed,thu,fri,sat
 * # |  |  |  |  |
 * # *  *  *  *  * user-name  command to be executed
 */
public final class CrontabExpression implements Comparable<CrontabExpression> {


  private final String rawExpression;
  private final int lineNumber;
  private final String executingUser;
  private final String command;

  private final ImmutableMap<ExpressionPart, ImmutableSortedSet<Integer>> expressionRuntimes;

  /**
   * Constructor
   * @param lineNumber the line number in the crontab
   * @param rawExpression the string value of the line as it appears in the crontab
   */
  public CrontabExpression(final int lineNumber, final String rawExpression) {
    checkNotNull(rawExpression, "rawExpression");

    checkArgument(lineNumber > 0, "lineNumber should be positive: %s", lineNumber);
    this.lineNumber = lineNumber;

    this.rawExpression = rawExpression.trim();
    checkArgument(!this.rawExpression.isEmpty(), "Empty expression");

    final List<String> expressionParts = Utils.WHITESPACE_SPLITTER.splitToList(this.rawExpression);

    checkArgument(expressionParts.size() >= ExpressionPart.values().length, "Uncommented line %s does not contain all expected parts: %s", lineNumber, rawExpression);

    this.executingUser = expressionParts.get(ExpressionPart.ExecutingUser.ordinal());

    // The command expression is everything after the user - just join it right back up with space separators
    // side-effect: collapses whitespace in the command - may break some commands out there that require lots of whitespace?
    this.command = Joiner.on(' ').join(Iterables.skip(expressionParts, ExpressionPart.values().length - 1));

    // Fill in the runtime schedule based on the cron expressions
    final HashMap<ExpressionPart, ImmutableSortedSet<Integer>> runtimes = Maps.newHashMap();

    for (ExpressionPart expressionPart : ExpressionPart.values()) {

      // Ignore anything starting with or coming after the user value
      if (expressionPart.ordinal() >= ExpressionPart.ExecutingUser.ordinal()) {
        continue;
      }

      runtimes.put(expressionPart, evaluateExpressionPart(expressionPart, expressionParts.get(expressionPart.ordinal())));
    }

    this.expressionRuntimes = ImmutableMap.copyOf(runtimes);
  }

  /**
   * Does the actual work of tearing apart the schedule expression and making them
   * into numerical sets of runtime whitelists
   *
   * @param expressionPart The current part we're working on
   * @param expression     The text expression to evaluate
   * @return A set within the expression's possible execution range
   */
  private static ImmutableSortedSet<Integer> evaluateExpressionPart(final ExpressionPart expressionPart, final String expression) {
    // Order of operations ->
    // 1) Split value by commas (lists) and for each csv.n:
    // 2) Split value by slashes (range/rangeStep)
    // 3) Match all for '*' or split hyphenated range for rangeStart and rangeEnd

    final List<String> csvParts = Utils.COMMA_SPLITTER.splitToList(expression);

    final TreeSet<Integer> results = Sets.newTreeSet();

    for (final String csvPart : csvParts) {

      final List<String> slashParts = Utils.FORWARD_SLASH_SPLITTER.splitToList(csvPart);

      // Range step of expression i.e. */2 (none is 1 obviously)
      int rangeStep = 1;

      checkArgument(!slashParts.isEmpty() && slashParts.size() <= 2, "Invalid cron expression for %s: %s", expressionPart.name(), expression);

      if (slashParts.size() == 2) {
        // Ordinal definition: 0 = rangeExpression, 1 = stepExpression
        final Integer rangeStepInteger = expressionPart.textUnitToInt(slashParts.get(1));

        checkNotNull(rangeStepInteger, "Invalid cron expression for %s (rangeStep is not a positive int): %s", expressionPart.name(), expression);

        checkArgument(rangeStepInteger > 0, "Invalid cron expression for %s (rangeStep is not valid): %s", expressionPart.name(), expression);

        rangeStep = rangeStepInteger;
      }

      final String rangeExpression = slashParts.get(0);

      final Range<Integer> allowedRange = expressionPart.getAllowedRange();

      int rangeStart = allowedRange.lowerEndpoint();
      int rangeEnd = allowedRange.upperEndpoint();

      // either * or 0 or 0-6, etc
      if (!"*".equals(rangeExpression)) {

        final List<String> hyphenParts = Utils.HYPHEN_SPLITTER.splitToList(rangeExpression);

        checkArgument(!hyphenParts.isEmpty() && hyphenParts.size() <= 2, "Invalid cron expression for %s: %s", expressionPart.name(), expression);

        Integer rangeStartInteger = expressionPart.textUnitToInt(hyphenParts.get(0));

        checkNotNull(rangeStartInteger, "Invalid cron expression for %s (rangeStart is not an int): %s", expressionPart.name(), expression);

        //correct terrible "sunday can be either 0 or 7" bug/feature in crond
        if (expressionPart == ExpressionPart.DaysOfWeek && rangeStartInteger == 7) {
          rangeStartInteger = 0;
        }

        checkArgument(allowedRange.contains(rangeStartInteger), "Invalid cron expression for %s (valid range is %s): %s", expressionPart.name(), expressionPart.getAllowedRange(), expression);

        rangeStart = rangeStartInteger;

        if (hyphenParts.size() == 2) {

          Integer rangeEndInteger = expressionPart.textUnitToInt(hyphenParts.get(1));

          checkNotNull(rangeEndInteger, "Invalid cron expression for %s (rangeEnd is not an int): %s", expressionPart.name(), expression);

          //correct terrible "sunday can be either 0 or 7" bug/feature in crond
          if (expressionPart == ExpressionPart.DaysOfWeek && rangeEndInteger == 7) {
            rangeEndInteger = 0;
          }

          checkArgument(allowedRange.contains(rangeEndInteger), "Invalid cron expression for %s (valid range is %s): %s", expressionPart.name(), expressionPart.getAllowedRange(), expression);

          rangeEnd = rangeEndInteger;

        } else {
          // Single value specified
          rangeEnd = rangeStart;

        }

      }

      for (int runTime = rangeStart; runTime <= rangeEnd; runTime += rangeStep) {
        results.add(runTime);
      }

    }

    return ImmutableSortedSet.copyOf(results);
  }

  public Set<Integer> getMinutes() { return this.expressionRuntimes.get(ExpressionPart.Minutes); }

  public SortedSet<Integer> getHours() {
    return this.expressionRuntimes.get(ExpressionPart.Hours);
  }

  public SortedSet<Integer> getDays() {
    return this.expressionRuntimes.get(ExpressionPart.DaysOfMonth);
  }

  public SortedSet<Integer> getMonths() { return this.expressionRuntimes.get(ExpressionPart.Months); }

  public SortedSet<Integer> getDaysOfWeek() {
    return this.expressionRuntimes.get(ExpressionPart.DaysOfWeek);
  }

  public String getRawExpression() {return this.rawExpression;}

  public String getExecutingUser() {return this.executingUser;}

  public String getCommand() {return this.command;}

  public int getLineNumber() {
    return lineNumber;
  }

  public boolean timeInSchedule(final LocalDateTime localDateTime) {
    checkNotNull(localDateTime, "localDateTime");

    // joda-time uses 1-7 dayOfWeek with Sunday as 7, so convert 7 to 0 to match allowed range of 0-6
    return getDaysOfWeek().contains(localDateTime.getDayOfWeek() == 7 ? 0 : localDateTime.getDayOfWeek())
      && getMonths().contains(localDateTime.getMonthOfYear())
      && getDays().contains(localDateTime.getDayOfMonth())
      && getHours().contains(localDateTime.getHourOfDay())
      && getMinutes().contains(localDateTime.getMinuteOfHour());
  }

  @Override
  public int hashCode() {
    return this.rawExpression.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof CrontabExpression
      && this.rawExpression.equalsIgnoreCase(((CrontabExpression) o).rawExpression);
  }

  @Override
  public String toString() {
    return String.format("[Line: %s] %s", this.lineNumber, this.rawExpression);
  }

  @Override
  public int compareTo(CrontabExpression o) {
    checkNotNull(o, "comparing null to CrontabExpression instance");

    return ComparisonChain.start()
      .compare(this.lineNumber, o.lineNumber)
      .compare(this.rawExpression, o.rawExpression)
      .result();
  }

}
