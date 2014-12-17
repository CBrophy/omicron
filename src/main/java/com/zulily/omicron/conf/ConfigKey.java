package com.zulily.omicron.conf;

public enum ConfigKey {
  CrontabPath("crontab.path", "/etc/crontab"),
  TimeZone("timezone", "UTC"),

  // Email config
  AlertEmailEnabled("alert.email.enabled", "false"),
  AlertEmailAddressTo("alert.email.address.to", "someone@example.com"),
  AlertEmailAddressFrom("alert.email.address.from", "someone@example.com"),
  AlertEmailSmtpHost("alert.email.smtp.host", "localhost"),
  AlertEmailSmtpPort("alert.email.smtp.port", "25"),

  AlertMinutesDelayRepeat("alert.minutes.delay.repeat", "20"),

  TaskDuplicateAllowedCount("task.duplicate.allowed.count", "2"),
  TaskCriticalReturnCode("task.critical.return.code", "100"), // expected to be between 0 and 255 according to bash man pages

  SLAMinutesSinceSuccess("sla.minutes.since.success", "60"),

  Unknown("", "");

  private final String rawName;
  private final String defaultValue;

  private ConfigKey(final String rawName, final String defaultValue) {
    this.rawName = rawName;
    this.defaultValue = defaultValue;
  }

  public static ConfigKey fromString(final String rawName) {
    if (rawName != null) {

      final String trimmed = rawName.trim();

      for (ConfigKey configKey : ConfigKey.values()) {

        if (configKey.rawName.equalsIgnoreCase(trimmed)) {
          return configKey;
        }

      }

    }

    return Unknown;
  }

  public String getRawName() {
    return rawName;
  }

  public String getDefaultValue() {
    return defaultValue;
  }
}