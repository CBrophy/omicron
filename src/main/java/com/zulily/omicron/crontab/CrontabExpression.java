package com.zulily.omicron.crontab;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

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
 *
 * i.e.
 * 1-10/2 * * * * -> returns 1,3,5,7,9 day vals
 *
 * # Example of job definition:
 * # .---------------- minute (0 - 59)
 * # |  .------------- hour (0 - 23)
 * # |  |  .---------- day of month (1 - 31)
 * # |  |  |  .------- month (1 - 12) OR jan,feb,mar,apr ...
 * # |  |  |  |  .---- day of week (0 - 7)  (Sunday=0 or 7) OR sun,mon,tue,wed,thu,fri,sat
 * # |  |  |  |  |
 * # *  *  *  *  * user-name  command to be executed
 */
public class CrontabExpression {
  public static final Splitter CRON_SPLITTER = Splitter.on(CharMatcher.WHITESPACE).trimResults().omitEmptyStrings();
  private static final Splitter COMMA_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
  private static final Splitter FORWARD_SLASH_SPLITTER = Splitter.on('/').trimResults().omitEmptyStrings();
  private static final Splitter HYPHEN_SPLITTER = Splitter.on('-').trimResults().omitEmptyStrings();

  private static enum ExpressionPart {
    Minutes(Range.closed(0, 59)),
    Hours(Range.closed(0, 23)),
    DaysOfMonth(Range.closed(1, 31)),
    Months(Range.closed(1, 12), new String[]{"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"}),
    DaysOfWeek(Range.closed(0, 6), new String[]{"sun", "mon", "tue", "wed", "thu", "fri", "sat"}),
    ExecutingUser,
    Command;

    private final Range<Integer> expressionRange;
    private final ImmutableMap<String, Integer> stringNameMap;

    private ExpressionPart() {
      this(null, null);
    }

    private ExpressionPart(final Range<Integer> expressionRange) {
      this(expressionRange, null);
    }

    private ExpressionPart(final Range<Integer> expressionRange, final String[] stringValues) {
      this.expressionRange = expressionRange;

      HashMap<String, Integer> stringValuesMap = Maps.newHashMap();

      if (stringValues != null && expressionRange != null) {
        for (int index = 0; index < stringValues.length; index++) {
          stringValuesMap.put(stringValues[index], index + expressionRange.lowerEndpoint()); // correct for 1-based month
        }
      }

      this.stringNameMap = ImmutableMap.copyOf(stringValuesMap);
    }


    Range<Integer> getExpressionRange() {
      return expressionRange;
    }

    Integer stringValueToInt(final String value) {
      Integer intValue = Ints.tryParse(value);

      if (intValue == null && stringNameMap != null) {
        intValue = stringNameMap.get(value.toLowerCase());
      }

      return intValue;
    }


  }

  private final String rawExpression;

  private final String executingUser;
  private final String command;

  private final ImmutableMap<ExpressionPart, ImmutableSortedSet<Integer>> expressionRuntimes;

  public CrontabExpression(final String rawExpression) {
    checkNotNull(rawExpression, "rawExpression");

    this.rawExpression = rawExpression.trim();

    checkArgument(!this.rawExpression.isEmpty(), "empty expression");

    List<String> expressionParts = CRON_SPLITTER.splitToList(this.rawExpression);

    checkArgument(expressionParts.size() >= ExpressionPart.values().length);

    this.executingUser = expressionParts.get(ExpressionPart.ExecutingUser.ordinal());

    // The command is everything after the user - just join it right back up with space separators
    // side-effect: collapses whitespace in the command - may break some commands out there that require lots of whitespace?
    this.command = Joiner.on(' ').join(Iterables.skip(expressionParts, ExpressionPart.values().length - 1));

    // Fill out the runtime schedule based on the cron expressions
    HashMap<ExpressionPart, ImmutableSortedSet<Integer>> runtimes = Maps.newHashMap();

    for (ExpressionPart expressionPart : ExpressionPart.values()) {

      if (expressionPart == ExpressionPart.ExecutingUser || expressionPart == ExpressionPart.Command) {
        continue;
      }

      runtimes.put(expressionPart, evaluateExpressionPart(expressionPart, expressionParts.get(expressionPart.ordinal())));
    }

    this.expressionRuntimes = ImmutableMap.copyOf(runtimes);
  }

  /**
   * Does the actual work of tearing apart the schedule parts and making them
   * into numerical sets
   *
   * @param expressionPart The current part we're working on
   * @param expression     The text expression to evaluate
   * @return A set within the expression's possible range
   */
  private static ImmutableSortedSet<Integer> evaluateExpressionPart(final ExpressionPart expressionPart, final String expression) {
    List<String> csvParts = COMMA_SPLITTER.splitToList(expression);

    TreeSet<Integer> results = Sets.newTreeSet();

    for (final String csvPart : csvParts) {

      List<String> slashParts = FORWARD_SLASH_SPLITTER.splitToList(csvPart);

      // Range step of expression i.e. */2 (none is 1 obviously)
      int rangeStep = 1;

      checkArgument(!slashParts.isEmpty() && slashParts.size() <= 2, "Invalid cron expression for %s: %s", expressionPart.name(), expression);


      if (slashParts.size() == 2) {
        // 0 = rangeExpression, 1 = stepExpression
        Integer rangeStepInteger = expressionPart.stringValueToInt(slashParts.get(1));

        checkNotNull(rangeStepInteger, "Invalid cron expression for %s (rangeStep is not a positive int): %s", expressionPart.name(), expression);

        checkArgument(rangeStepInteger > 0, "Invalid cron expression for %s (rangeStep is not valid): %s", expressionPart.name(), expression);

        rangeStep = rangeStepInteger;
      }

      String rangeExpression = slashParts.get(0);
      Range<Integer> allowedRange = expressionPart.getExpressionRange();

      int rangeStart = allowedRange.lowerEndpoint();
      int rangeEnd = allowedRange.upperEndpoint();

      // either * or 0 or 0-6, etc
      if (!"*".equals(rangeExpression)) {

        List<String> hyphenParts = HYPHEN_SPLITTER.splitToList(rangeExpression);

        checkArgument(!hyphenParts.isEmpty() && hyphenParts.size() <= 2, "Invalid cron expression for %s: %s", expressionPart.name(), expression);

        Integer rangeStartInteger = expressionPart.stringValueToInt(hyphenParts.get(0));

        checkNotNull(rangeStartInteger, "Invalid cron expression for %s (rangeStart is not an int): %s", expressionPart.name(), expression);

        //correct terrible "sunday can be either 0 or 7" bug/feature in crond
        if (expressionPart == ExpressionPart.DaysOfWeek && rangeStartInteger == 7) {
          rangeStartInteger = 0;
        }

        checkArgument(allowedRange.contains(rangeStartInteger), "Invalid cron expression for %s (rangeStart is not valid): %s", expressionPart.name(), expression);

        rangeStart = rangeStartInteger;

        if (hyphenParts.size() == 2) {

          Integer rangeEndInteger = expressionPart.stringValueToInt(hyphenParts.get(1));

          checkNotNull(rangeEndInteger, "Invalid cron expression for %s (rangeEnd is not an int): %s", expressionPart.name(), expression);

          //correct terrible "sunday can be either 0 or 7" bug/feature in crond
          if (expressionPart == ExpressionPart.DaysOfWeek && rangeEndInteger == 7) {
            rangeEndInteger = 0;
          }

          checkArgument(allowedRange.contains(rangeEndInteger), "Invalid cron expression for %s (rangeEnd is not valid): %s", expressionPart.name(), expression);

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
}
