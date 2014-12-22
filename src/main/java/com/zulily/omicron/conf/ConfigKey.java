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

/**
 * This enum represents the available defined values for
 * configuring omicron, as well as the defaults for those values
 *
 * Omicron allows individual rows in the crontab to specify overrides
 * for the global config. Whether or not omicron will honor an override
 * is determined by the allowOverride property on each ConfigKey entry
 */
public enum ConfigKey {
  CrontabPath("crontab.path", "/etc/crontab", false),
  TimeZone("timezone", "UTC", false),

  // Email config
  AlertEmailEnabled("alert.email.enabled", "false", true),
  AlertEmailAddressTo("alert.email.address.to", "someone@example.com", false),
  AlertEmailAddressFrom("alert.email.address.from", "someone@example.com", false),
  AlertEmailSmtpHost("alert.email.smtp.host", "localhost", false),
  AlertEmailSmtpPort("alert.email.smtp.port", "25", false),

  AlertMinutesDelayRepeat("alert.minutes.delay.repeat", "20", true),

  TaskDuplicateAllowedCount("task.duplicate.allowed.count", "2", true),
  TaskCriticalReturnCode("task.critical.return.code", "100", true), // expected to be between 0 and 255 according to bash man pages

  SLAMinutesSinceSuccess("sla.minutes.since.success", "60", true),

  Unknown("", "", false);

  private final String rawName;
  private final String defaultValue;
  private final boolean allowOverride;

  private ConfigKey(final String rawName, final String defaultValue, final boolean allowOverride) {
    this.rawName = rawName;
    this.defaultValue = defaultValue;
    this.allowOverride = allowOverride;
  }

  /**
   * Returns a ConfigKey that best matches the provided string name
   * or Unknown if the value is not recognized
   * @param rawName The text name of the config key
   * @return A ConfigKey value, or Unknown if the config key name cannot be matched
   */
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

  /**
   * @return The raw name of the config key as it appears in the config file
   */
  public String getRawName() {
    return rawName;
  }

  /**
   * @return The default value of the config key if it is omitted from config
   */
  public String getDefaultValue() {
    return defaultValue;
  }

  /**
   * @return True if the config key can be overridden in the crontab, false otherwise
   */
  public boolean allowOverride() {
    return allowOverride;
  }
}