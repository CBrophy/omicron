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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.primitives.Ints;

import java.util.HashMap;

/**
 * Enum representing the parts of a crontab schedule and the explicit ranges associated with each
 */
enum ExpressionPart {
  Minutes(Range.closed(0, 59)),
  Hours(Range.closed(0, 23)),
  DaysOfMonth(Range.closed(1, 31)),
  Months(Range.closed(1, 12), new String[]{"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"}),
  DaysOfWeek(Range.closed(0, 6), new String[]{"sun", "mon", "tue", "wed", "thu", "fri", "sat"}),
  ExecutingUser,
  Command;

  private final Range<Integer> allowedRange;
  private final ImmutableMap<String, Integer> stringNameMap;

  ExpressionPart() {
    this(null, null);
  }

  ExpressionPart(final Range<Integer> allowedRange) {
    this(allowedRange, null);
  }

  ExpressionPart(final Range<Integer> allowedRange, final String[] stringValues) {
    this.allowedRange = allowedRange;

    final HashMap<String, Integer> stringValuesMap = Maps.newHashMap();

    if (stringValues != null && allowedRange != null) {

      for (int index = 0; index < stringValues.length; index++) {
        stringValuesMap.put(stringValues[index], index + allowedRange.lowerEndpoint()); // correct for 1-based month/d-o-m
      }

    }

    this.stringNameMap = ImmutableMap.copyOf(stringValuesMap);
  }


  Range<Integer> getAllowedRange() {
    return allowedRange;
  }

  /**
   * This function transforms text values such as 'sun' or 'jul'
   * into the respective index values
   *
   * @param value The text unit to translate
   * @return The int value of the text unit
   */
  Integer textUnitToInt(final String value) {
    Integer intValue = Ints.tryParse(value);

    if (intValue == null && stringNameMap != null) {
      intValue = stringNameMap.get(value.toLowerCase());
    }

    return intValue;
  }


}