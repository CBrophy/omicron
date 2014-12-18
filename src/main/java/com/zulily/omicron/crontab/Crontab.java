package com.zulily.omicron.crontab;

import com.google.common.base.CharMatcher;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.zulily.omicron.conf.ConfigKey;
import com.zulily.omicron.conf.Configuration;
import com.zulily.omicron.Utils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.zulily.omicron.Utils.error;
import static com.zulily.omicron.Utils.info;
import static com.zulily.omicron.Utils.warn;

public final class Crontab {
  public final static String OVERRIDE = "#override:";

  private final ImmutableSet<CrontabExpression> crontabExpressions;
  private final ImmutableMap<String, String> variables;
  private final ImmutableMap<Integer, Configuration> configurationOverrides;
  private final int badRowCount;
  private final long lastModified;

  public Crontab(final Configuration configuration) {

    checkNotNull(configuration, "configuration");
    checkState(Utils.fileExistsAndCanRead(configuration.getCrontab()), "Cannot read/find crontab: ", configuration.getCrontab().getAbsolutePath());

    HashMap<String, String> variableMap = Maps.newHashMap();
    HashMap<Integer, Configuration> rawOverrideMap = Maps.newHashMap();
    HashSet<CrontabExpression> results = Sets.newHashSet();

    int bad = 0;

    this.lastModified = configuration.getCrontab().lastModified();

    try {
      int lineNumber = 0;

      final ImmutableList<String> lines = Files.asCharSource(configuration.getCrontab(), Charset.defaultCharset()).readLines();
      ImmutableMap<ConfigKey, String> overrideMap = null;

      for (final String line : lines) {
        lineNumber++;

        final String trimmed = line.trim();

        if (trimmed.isEmpty()) {
          continue;
        }

        // Skip commented lines
        if (line.startsWith(OVERRIDE)) {
          overrideMap = getOverrideConfiguration(line);

          info("[Line: {0}] Loaded {1} overrides for next task line", String.valueOf(lineNumber), String.valueOf(overrideMap.size()));

          continue;
        }

        if ('#' == trimmed.charAt(0)) {

          if (overrideMap != null) {

            warn("[Line: {0}] The previous override map will be ignored because this line is commented", String.valueOf(lineNumber));

            overrideMap = null;
          }

          continue;
        }

        // If it's a variable assignment, save it in the map
        final List<String> variableParts = getVariable(trimmed);

        if (variableParts.size() == 2) {
          variableMap.put(variableParts.get(0), variableParts.get(1));
          continue;
        }

        try {

          results.add(new CrontabExpression(lineNumber, trimmed));

          // The previous non-blank/commented line is an unassociated override map. Associate with this row
          if (overrideMap != null) {

            rawOverrideMap.put(lineNumber, configuration.withOverrides(overrideMap));

            overrideMap = null;
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
    this.variables = ImmutableMap.copyOf(variableMap);
    this.crontabExpressions = ImmutableSet.copyOf(results);
    this.configurationOverrides = ImmutableMap.copyOf(rawOverrideMap);
  }

  private ImmutableMap<ConfigKey, String> getOverrideConfiguration(final String line) {

    final HashMap<ConfigKey, String> result = Maps.newHashMap();

    final String noPrefix = line.substring(OVERRIDE.length()).trim();

    final List<String> overrideList = Utils.CSV_SPLITTER.splitToList(noPrefix);

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

      result.put(configKey, overrideParts.get(1));

    }

    return ImmutableMap.copyOf(result);
  }

  private static List<String> getVariable(final String line) {
    final int firstEqualIndex = line.indexOf('=');

    if (firstEqualIndex == -1) {
      return ImmutableList.of();
    }

    final int firstQuoteIndex = line.indexOf('"');

    final String varName = line.substring(0, firstEqualIndex);

    // Variable names cannot contain whitespace
    if (CharMatcher.WHITESPACE.matchesAnyOf(varName)) {
      return ImmutableList.of();
    }

    String varValue = null;

    if (firstQuoteIndex > -1) {

      // If first quote comes before first equal sign, cannot be a var assignment
      if (firstQuoteIndex < firstEqualIndex) {
        return ImmutableList.of();
      }

      varValue = line.substring(firstQuoteIndex + 1, line.lastIndexOf('"'));
    } else {
      varValue = line.substring(firstEqualIndex + 1, line.length());
    }

    return ImmutableList.of("$" + varName, varValue);
  }

  public ImmutableSet<CrontabExpression> getCrontabExpressions() {
    return crontabExpressions;
  }

  public int getBadRowCount() {
    return badRowCount;
  }

  public long getLastModified() {
    return lastModified;
  }

  public ImmutableMap<String, String> getVariables() {
    return variables;
  }

  public ImmutableMap<Integer, Configuration> getConfigurationOverrides() {
    return configurationOverrides;
  }
}
