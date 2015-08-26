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

import com.google.common.base.CharMatcher;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.zulily.omicron.Utils;
import com.zulily.omicron.conf.ConfigKey;
import com.zulily.omicron.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.zulily.omicron.Utils.COMMA_JOINER;
import static com.zulily.omicron.Utils.error;
import static com.zulily.omicron.Utils.info;
import static com.zulily.omicron.Utils.warn;

/**
 * This class contains the logic of reading the specified crontab into memory.
 * <p>
 * Schedule rows are read in an represented as {@link com.zulily.omicron.crontab.CrontabExpression} types
 * Override rows are read in and associated with the next row that is not blank. If the next non-blank row is commented,
 * then the override will be ignored.
 */
public final class Crontab {
  public final static String OVERRIDE_KEYWORD = "#override:";

  private final ImmutableSet<CrontabExpression> crontabExpressions;
  private final ImmutableList<CronVariable> variableList;
  private final ImmutableMap<Integer, Configuration> configurationOverrides;
  private final int badRowCount;
  private final long crontabTimestamp;

  /**
   * Constructor
   *
   * @param configuration The global configuration object for Omicron
   */
  public Crontab(final Configuration configuration) {

    checkNotNull(configuration, "configuration");

    final File crontabFile = new File(configuration.getString(ConfigKey.CrontabPath));

    checkState(Utils.fileExistsAndCanRead(crontabFile), "Cannot read/find crontab: ", crontabFile.getAbsolutePath());

    final List<CronVariable> cronVariables = Lists.newArrayList();
    final HashMap<Integer, Configuration> rawOverrideMap = Maps.newHashMap();
    final HashSet<CrontabExpression> results = Sets.newHashSet();

    int bad = 0;

    this.crontabTimestamp = crontabFile.lastModified();

    try {
      int lineNumber = 0;

      final ImmutableList<String> lines = Files.asCharSource(crontabFile, Charset.defaultCharset()).readLines();

      ImmutableMap<ConfigKey, String> overrideMap = null;

      for (final String line : lines) {
        lineNumber++;

        final String trimmed = line.trim();

        if (trimmed.isEmpty()) {
          continue;
        }

        if (line.startsWith(OVERRIDE_KEYWORD)) {

          overrideMap = getOverrideConfiguration(line);

          continue;
        }

        // If it's a variable assignment, save it in the map
        // and skip to the next row
        final CronVariable cronVariable = getVariable(trimmed);

        if (cronVariable != null) {
          info("[Line: {0}] Found variable definition: {1} -> {2}", String.valueOf(lineNumber), cronVariable.getName(), cronVariable.getValue());
          cronVariables.add(cronVariable);
          continue;
        }

        try {

          final CrontabExpression crontabExpression = new CrontabExpression(lineNumber, trimmed);

          if (crontabExpression.isCommented() && crontabExpression.isMalformed()) {
            info("[Line: {0}] Skipping general comment: {1}", String.valueOf(lineNumber), line);
            continue;
          }

          // crontabExpression.isCommented() || crontabExpression.isMalformed() || normal expression
          // Commented rows that successfully parse as expressions are loaded anyways, to
          // allow for alerting of "forgotten" disabled tasks
          // Likewise, uncommented but malformed rows are also loaded so that malformed
          // alerting can be done on them

          results.add(crontabExpression);

          // The previous non-blank/commented line is an unassociated override map. Associate with this row
          if (overrideMap != null) {

            info("[Line: {0}] Adding schedule with config overrides \"{1}\": {2}", String.valueOf(lineNumber), getOverrideMapString(overrideMap), crontabExpression.getCommand());

            rawOverrideMap.put(lineNumber, configuration.withOverrides(overrideMap));

            overrideMap = null;
          } else {
            info("[Line: {0}] Adding schedule: {1}", String.valueOf(lineNumber), crontabExpression.getCommand());
          }

        } catch (Exception e) {
          bad++;
          error("[Line: {0}] Failed to read crontab entry: {1}\n{2}", String.valueOf(lineNumber), trimmed, Throwables.getStackTraceAsString(e));
        }

      }

    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    this.badRowCount = bad;
    this.variableList = ImmutableList.copyOf(cronVariables);
    this.crontabExpressions = ImmutableSet.copyOf(results);
    this.configurationOverrides = ImmutableMap.copyOf(rawOverrideMap);
  }

  private ImmutableMap<ConfigKey, String> getOverrideConfiguration(final String line) {

    // Override configuration line ->
    // #override: <ConfigKey>=value,...
    final HashMap<ConfigKey, String> result = Maps.newHashMap();

    final String noPrefix = line.substring(OVERRIDE_KEYWORD.length()).trim();

    final List<String> overrideList = Utils.COMMA_SPLITTER.splitToList(noPrefix);

    for (final String override : overrideList) {
      List<String> overrideParts = Utils.EQUAL_SPLITTER.splitToList(override);

      if (overrideParts.size() != 2) {
        warn("Malformed override: {0}", line);
        continue;
      }

      final ConfigKey configKey = ConfigKey.fromString(overrideParts.get(0));

      if (configKey == ConfigKey.Unknown) {
        warn("Malformed override: {0}", line);
        continue;
      }

      if (!configKey.allowOverride()) {
        warn("Cannot override {0}: {1}", configKey.getRawName(), line);
        continue;
      }

      result.put(configKey, overrideParts.get(1));

    }

    return ImmutableMap.copyOf(result);
  }

  private static CronVariable getVariable(final String line) {
    // Crontab variables like ->
    // <VARNAME>=value

    final int firstEqualIndex = line.indexOf('=');

    if (firstEqualIndex == -1) {
      return null;
    }

    final int firstQuoteIndex = line.indexOf('"');

    // varname is the entire value between the first non-whitespace char
    // and the first equal sign
    final String varName = line.substring(0, firstEqualIndex);

    // Variable names cannot contain whitespace
    if (CharMatcher.WHITESPACE.matchesAnyOf(varName)) {
      return null;
    }

    String varValue;

    // var values can be quoted strings
    // in such a case, the var value is everything between the first quote
    // and the last
    if (firstQuoteIndex > -1) {

      // If first quote comes before first equal sign, cannot be a var assignment
      if (firstQuoteIndex < firstEqualIndex) {
        return null;
      }

      varValue = line.substring(firstQuoteIndex + 1, line.lastIndexOf('"'));
    } else {
      varValue = line.substring(firstEqualIndex + 1, line.length());
    }

    return new CronVariable(varName, varValue);
  }

  /**
   * @return A set of {@link com.zulily.omicron.crontab.CrontabExpression} objects read from the crontab
   */
  public ImmutableSet<CrontabExpression> getCrontabExpressions() {
    return crontabExpressions;
  }

  /**
   * @return the number of crontab schedule rows that were considered 'bad' and cannot be executed
   */
  public int getBadRowCount() {
    return badRowCount;
  }

  /**
   * @return The last modified timestamp of the crontab file
   */
  public long getCrontabTimestamp() {
    return crontabTimestamp;
  }

  /**
   * @return A map of the variables defined in the crontab
   */
  public ImmutableList<CronVariable> getVariables() {
    return variableList;
  }

  /**
   * @return A map of Configuration overrides by the line number they are associated with
   */
  public ImmutableMap<Integer, Configuration> getConfigurationOverrides() {
    return configurationOverrides;
  }

  private String getOverrideMapString(final ImmutableMap<ConfigKey, String> overrideMap) {
    return COMMA_JOINER.join(overrideMap.entrySet()
        .stream()
        .map(entry -> entry.getKey().getRawName() + "->" + entry.getValue())
        .collect(Collectors.toList())
    );
  }
}
