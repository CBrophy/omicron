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

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.zulily.omicron.Utils;
import org.joda.time.Chronology;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.zulily.omicron.Utils.info;
import static com.zulily.omicron.Utils.warn;

/**
 * Configuration reads and stores values from the global config file
 * which can be specified on the command line when omicron is launched.
 * <p/>
 * Individual rows in the crontab can override certain values in the global config
 * <p/>
 * See: {@link com.zulily.omicron.conf.ConfigKey} for more details
 */
public class Configuration {
  public final static long DEFAULT_TIMESTAMP = 0L;

  private final ImmutableMap<ConfigKey, String> rawConfigMap;

  private final File crontab;

  private final Chronology chronology;

  /**
   * Constructor
   *
   * @param configFile The config file to read from
   */
  public Configuration(final File configFile) {
    this(loadConfig(configFile));
  }

  private Configuration(final ImmutableMap<ConfigKey, String> rawConfigMap) {
    this.rawConfigMap = checkNotNull(rawConfigMap, "rawComfigMap");

    this.crontab = new File(getString(ConfigKey.CrontabPath));

    final DateTimeZone timeZone = DateTimeZone.forID(getString(ConfigKey.TimeZone));

    this.chronology = ISOChronology.getInstance(timeZone);

    this.printConfig();
  }

  /**
   * Extract a new instance based off of this one but with some of the config values being updated
   *
   * @param overrideMap ConfigKey values to override
   * @return An instance with the updated config values
   */
  public Configuration withOverrides(final ImmutableMap<ConfigKey, String> overrideMap) {
    if (overrideMap == null || overrideMap.isEmpty()) {
      return this;
    }

    HashMap<ConfigKey, String> result = Maps.newHashMap(this.rawConfigMap);

    result.putAll(overrideMap);

    return new Configuration(ImmutableMap.copyOf(result));
  }

  private void printConfig() {
    for (ConfigKey configKey : ConfigKey.values()) {

      if (configKey == ConfigKey.Unknown) {
        continue;
      }

      info("{0} = {1}", configKey.getRawName(), getString(configKey));
    }

  }

  private static ImmutableMap<ConfigKey, String> loadConfig(final File configFile) {

    if (!Utils.fileExistsAndCanRead(configFile)) {

      info(configFile == null ? "No config file specified. Will use defaults." : "Config file not found, not a file, or cannot be read. Will use defaults.");

      return ImmutableMap.of();
    }

    final Splitter equalSplitter = Splitter.on('=').trimResults().omitEmptyStrings();
    final HashMap<ConfigKey, String> config = Maps.newHashMap();

    try {

      ImmutableList<String> configLines = Files.asCharSource(configFile, Charset.defaultCharset()).readLines();

      for (final String configLine : configLines) {

        final String trimmed = configLine.trim();

        //Skip commented/blank lines
        if (trimmed.isEmpty() || '#' == trimmed.charAt(0)) {
          continue;
        }

        final List<String> configLineParts = equalSplitter.splitToList(trimmed);

        if (configLineParts.size() != 2) {

          warn("Skipping malformed config line: {0}", trimmed);
          continue;

        }

        final ConfigKey configKey = ConfigKey.fromString(configLineParts.get(0));

        if (configKey == ConfigKey.Unknown) {

          warn("Skipping unknown config param: {0}", trimmed);
          continue;

        }

        config.put(configKey, configLineParts.get(1));

      }

    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    if (config.isEmpty()) {
      warn("Config file values not loaded. Will use defaults.");
    }

    return ImmutableMap.copyOf(config);
  }

  /**
   * @return A file representing the crontab to read
   */
  public File getCrontab() {
    return crontab;
  }

  /**
   * @return The chronology to interpret the crontab schedule under
   */
  public Chronology getChronology() {
    return chronology;
  }

  /**
   * Reads a specified Configkey value as an int
   *
   * @param configKey The ConfigKey to get
   * @return An 'int' representation of the configured value
   */
  public int getInt(final ConfigKey configKey) {
    return Integer.parseInt(getString(configKey, rawConfigMap));
  }

  /**
   * Reads a specified Configkey value as a boolean
   *
   * @param configKey The ConfigKey to get
   * @return A 'boolean' representation of the configured value
   */
  public boolean getBoolean(final ConfigKey configKey) {
    return Boolean.parseBoolean(getString(configKey, rawConfigMap));
  }

  /**
   * Reads a specified ConfigKey value as a String
   *
   * @param configKey The ConfigKey to get
   * @return A 'String' representation of the configured value
   */
  public String getString(final ConfigKey configKey) {
    return getString(configKey, rawConfigMap);
  }

  private static String getString(final ConfigKey configKey, final Map<ConfigKey, String> configMap) {
    checkArgument(configKey != ConfigKey.Unknown, "Cannot get unknown config value");

    final String configValue = configMap.get(configKey);

    return configValue == null ? configKey.getDefaultValue() : configValue;
  }

}
