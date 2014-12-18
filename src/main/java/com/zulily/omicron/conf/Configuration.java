package com.zulily.omicron.conf;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.zulily.omicron.Utils;
import com.zulily.omicron.alert.Email;
import org.joda.time.Chronology;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;


import javax.mail.internet.AddressException;
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

public class Configuration {
  public final static long DEFAULT_TIMESTAMP = 0L;

  private final ImmutableMap<ConfigKey, String> rawConfigMap;
  private final File crontab;
  private final Email alertEmail;
  private final Chronology chronology;

  public Configuration(final File configFile) {
    this(loadConfig(configFile));
  }

  private Configuration(final ImmutableMap<ConfigKey, String> rawConfigMap) {
    this.rawConfigMap = checkNotNull(rawConfigMap, "rawComfigMap");

    final Splitter commaSplitter = Splitter.on(',').trimResults().omitEmptyStrings();

    this.crontab = new File(getString(ConfigKey.CrontabPath));

    try {

      this.alertEmail = Email.from(getString(ConfigKey.AlertEmailAddressFrom))
        .to(commaSplitter.split(getString(ConfigKey.AlertEmailAddressTo)))
        .withSMTPServer(getString(ConfigKey.AlertEmailSmtpHost), Integer.parseInt(getString(ConfigKey.AlertEmailSmtpPort)))
        .build();

    } catch (AddressException e) {
      throw Throwables.propagate(e);
    }

    final DateTimeZone timeZone = DateTimeZone.forID(getString(ConfigKey.TimeZone));

    this.chronology = ISOChronology.getInstance(timeZone);

    this.printConfig();
  }

  public Configuration withOverrides(final ImmutableMap<ConfigKey, String> overrideMap) {
    if (overrideMap == null || overrideMap.isEmpty()) {
      return this;
    }

    HashMap<ConfigKey, String> result = Maps.newHashMap(this.rawConfigMap);

    result.putAll(overrideMap);

    return new Configuration(ImmutableMap.copyOf(overrideMap));
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

  public File getCrontab() {
    return crontab;
  }

  public Email getAlertEmail() {
    return alertEmail;
  }

  public Chronology getChronology() {
    return chronology;
  }

  public int getInt(final ConfigKey configKey) {
    return Integer.parseInt(getString(configKey, rawConfigMap));
  }

  public boolean getBoolean(final ConfigKey configKey){
    return Boolean.parseBoolean(getString(configKey, rawConfigMap));
  }

  public String getString(final ConfigKey configKey) {
    return getString(configKey, rawConfigMap);
  }

  private static String getString(final ConfigKey configKey, final Map<ConfigKey, String> configMap) {
    checkArgument(configKey != ConfigKey.Unknown, "Cannot get unknown config value");

    final String configValue = configMap.get(configKey);

    return configValue == null ? configKey.getDefaultValue() : configValue;
  }

}
