package com.zulily.omicron;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
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


import static com.google.common.base.Preconditions.checkArgument;

public class Configuration {

  private static enum ConfigKey {
    CrontabPath("crontab.path", "/etc/crontab"),
    StatsFilePath("statsfile.path", "/tmp/omicronstats"),
    AlertAddressTo("alert.address.to", "cbrophy@zulily.com"),
    AlertAddressFrom("alert.address.from", "relevancy-robots@zulily.com"),
    AlertSmtpHost("alert.smtp.host", "localhost"),
    AlertSmtpPort("alert.smtp.port", "25"),
    TaskDuplicateAllowedCount("task.duplicate.allowed.count", "2"),
    TaskReturnCodeCriticalFailureThreshold("task.return.code.critical.failure.threshold", "100"), // must be between 0 and 255
    TimeZone("timezone", "UTC"),
    Unknown("", "");

    private final String rawName;
    private final String defaultValue;

    private ConfigKey(final String rawName, final String defaultValue) {
      this.rawName = rawName;
      this.defaultValue = defaultValue;
    }

    static ConfigKey fromString(final String rawName) {
      if (rawName != null) {

        String trimmed = rawName.trim();

        for (ConfigKey configKey : ConfigKey.values()) {

          if (configKey.rawName.equalsIgnoreCase(trimmed)) {
            return configKey;
          }

        }

      }

      return Unknown;
    }
  }

  private final ImmutableMap<ConfigKey, String> rawConfigMap;
  private final File crontab;
  private final File statsFile;
  private final Email alertEmail;
  private final int taskDuplicateAllowedCount;
  private final int taskReturnCodeCriticalFailureThreshold;
  private final Chronology chronology;

  Configuration(final File configFile) {
    rawConfigMap = loadConfig(configFile);

    Splitter commaSplitter = Splitter.on(',').trimResults().omitEmptyStrings();

    this.crontab = new File(getConfigValue(ConfigKey.CrontabPath));
    this.statsFile = new File(getConfigValue(ConfigKey.StatsFilePath));

    try {
      this.alertEmail = Email.from(getConfigValue(ConfigKey.AlertAddressFrom))
        .to(commaSplitter.split(getConfigValue(ConfigKey.AlertAddressTo)))
        .withSMTPServer(getConfigValue(ConfigKey.AlertSmtpHost), Integer.parseInt(getConfigValue(ConfigKey.AlertSmtpPort)))
        .build();
    } catch (AddressException e) {
      throw Throwables.propagate(e);
    }

    this.taskDuplicateAllowedCount = Math.abs(Integer.parseInt(getConfigValue(ConfigKey.TaskDuplicateAllowedCount)));
    this.taskReturnCodeCriticalFailureThreshold = Math.abs(Integer.parseInt(getConfigValue(ConfigKey.TaskReturnCodeCriticalFailureThreshold)));

    DateTimeZone timeZone = DateTimeZone.forID(getConfigValue(ConfigKey.TimeZone));
    this.chronology = ISOChronology.getInstance(timeZone);

    this.printConfig();
  }

  private void printConfig() {
    System.out.println("Loaded config:");

    for (ConfigKey configKey : ConfigKey.values()) {
      if (configKey == ConfigKey.Unknown) {
        continue;
      }

      System.out.println(String.format("%s = %s", configKey.rawName, getConfigValue(configKey)));
    }

  }


  private String getConfigValue(final ConfigKey configKey) {
    checkArgument(configKey != ConfigKey.Unknown, "Cannot get unknown config value");

    String configValue = rawConfigMap.get(configKey);

    return configValue == null ? configKey.defaultValue : configValue;
  }

  private static ImmutableMap<ConfigKey, String> loadConfig(final File configFile) {

    if (!Utils.fileExistsAndCanRead(configFile)) {
      System.out.println(
        configFile == null ? "No config file specified. Will use defaults." : "Config file not found, not a file, or cannot be read. Will use defaults.");
      return ImmutableMap.of();
    }

    Splitter equalSplitter = Splitter.on('=').trimResults().omitEmptyStrings();
    HashMap<ConfigKey, String> config = Maps.newHashMap();

    try {
      ImmutableList<String> configLines = Files.asCharSource(configFile, Charset.defaultCharset()).readLines();

      for (String configLine : configLines) {
        String trimmed = configLine.trim();

        //Skip commented/blank lines
        if (trimmed.isEmpty() || '#' == trimmed.charAt(0)) {
          continue;
        }

        List<String> configLineParts = equalSplitter.splitToList(trimmed);

        if (configLineParts.size() != 2) {
          System.out.println("Skipping malformed config line: " + trimmed);
          continue;
        }

        ConfigKey configKey = ConfigKey.fromString(configLineParts.get(0));

        if (configKey == ConfigKey.Unknown) {
          System.out.println("Skipping unknown config param: " + trimmed);
          continue;
        }

        config.put(configKey, configLineParts.get(1));

      }

    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    if (config.isEmpty()) {
      System.out.println("Config file values not loaded. Will use defaults.");
    }

    return ImmutableMap.copyOf(config);
  }

  public File getCrontab() {
    return crontab;
  }

  public File getStatsFile() {
    return statsFile;
  }

  public Email getAlertEmail() {
    return alertEmail;
  }

  public int getTaskDuplicateAllowedCount() {
    return taskDuplicateAllowedCount;
  }

  public int getTaskReturnCodeCriticalFailureThreshold() {
    return taskReturnCodeCriticalFailureThreshold;
  }

  public Chronology getChronology() {
    return chronology;
  }


}
