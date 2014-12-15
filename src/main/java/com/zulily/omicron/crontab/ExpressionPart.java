package com.zulily.omicron.crontab;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.primitives.Ints;

import java.util.HashMap;

enum ExpressionPart {
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