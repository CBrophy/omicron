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
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.zulily.omicron.Utils;
import org.joda.time.Chronology;
import org.joda.time.LocalTime;

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

  private final ImmutableMap<ConfigKey, String> rawConfigMap;
  private final long configurationTimestamp;
  private final String configFilePath;


  /**
   * Constructor
   *
   * @param configFilePath The config file to read from
   */
  public Configuration(final String configFilePath) {

    this(
      loadConfig(configFilePath),
      Utils.getTimestampFromPath(configFilePath),
      configFilePath);

    this.printConfig();
  }

  private Configuration(

    final ImmutableMap<ConfigKey, String> rawConfigMap,
    final long configurationTimestamp,
    final String configFilePath) {

    this.rawConfigMap = checkNotNull(rawConfigMap, "rawComfigMap");
    this.configurationTimestamp = configurationTimestamp;
    this.configFilePath = checkNotNull(configFilePath, "configFilePath");
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

    return new Configuration(
      ImmutableMap.copyOf(result),
      this.getConfigurationTimestamp(),
      this.configFilePath);
  }

  private void printConfig() {
    for (ConfigKey configKey : ConfigKey.values()) {

      if (configKey == ConfigKey.Unknown) {
        continue;
      }

      info("{0} = {1}", configKey.getRawName(), getString(configKey));
    }

  }

  private static ImmutableMap<ConfigKey, String> loadConfig(final String configFilePath) {

    if (Strings.isNullOrEmpty(configFilePath.trim())) {

      info("No config file specified. Will use defaults.");

      return ImmutableMap.of();

    }

    final File configFile = new File(configFilePath);

    if (!Utils.fileExistsAndCanRead(configFile)) {

      info("Config file not found, not a file, or cannot be read. Will use defaults.");

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

  /**
   * This function is used to update the config value map
   * when the file artifact is changed but the path doesn't change
   *
   * @return a new instance with recent config values
   */
  public Configuration reload() {
    return new Configuration(this.configFilePath);
  }

  /**
   * @return The lastModified timestamp of the loaded config file, or DEFAULT_TIMESTAMP
   * if no file is loaded
   */
  public long getConfigurationTimestamp() {
    return configurationTimestamp;
  }

  /**
   * @return The path to the config file used by Omicron
   */
  public String getConfigFilePath() {
    return configFilePath;
  }

  /**
   * @return The chronology to interpret the crontab schedule under
   */
  public Chronology getChronology() {
    return Utils.timeZoneToChronology(getString(ConfigKey.TimeZone));
  }

  @Override
  public boolean equals(Object o){
    return o instanceof Configuration
      && this.configurationTimestamp == ((Configuration) o).getConfigurationTimestamp()
      && configValuesMatch((Configuration) o);
  }

  private boolean configValuesMatch(final Configuration configuration){

    for (ConfigKey configKey : ConfigKey.values()) {
      if(configKey == ConfigKey.Unknown){
        continue;
      }

      if(!getString(configKey).equals(configuration.getString(configKey))){
        return false;
      }
    }

    return true;
  }

  public TimeInterval getTimeInterval(final ConfigKey configKey){

    final String configValue = getString(configKey);

    final int plusIndex = configValue.indexOf('+');

    if(configValue.isEmpty()) return null;

    final LocalTime startTime = LocalTime.parse(configValue.substring(0, plusIndex));

    final int hours = Integer.parseInt(configValue.substring(plusIndex + 1));

    return new TimeInterval(startTime, hours);

  }
}
