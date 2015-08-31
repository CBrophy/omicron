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
package com.zulily.omicron;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import java.io.File;
import java.net.InetAddress;
import java.time.format.DateTimeFormatter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A collection of utilities and shared static instances to support omicron
 */
public final class Utils {
  private final static Logger LOG = Logger.getGlobal();

  public final static long DEFAULT_TIMESTAMP = 0L;

  public final static Splitter COMMA_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
  public final static Joiner COMMA_JOINER = Joiner.on(',').skipNulls();
  public final static Splitter EQUAL_SPLITTER = Splitter.on('=').trimResults().omitEmptyStrings();
  public final static Splitter DOT_SPLITTER = Splitter.on('.').trimResults().omitEmptyStrings();
  public final static Splitter WHITESPACE_SPLITTER = Splitter.on(CharMatcher.WHITESPACE).trimResults().omitEmptyStrings();
  public final static Splitter FORWARD_SLASH_SPLITTER = Splitter.on('/').trimResults().omitEmptyStrings();
  public final static Splitter HYPHEN_SPLITTER = Splitter.on('-').trimResults().omitEmptyStrings();
  public final static DateTimeFormatter MESSAGE_DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm VV");


  /**
   * Shortcut method to determine if a file object can actually be "read" as a file
   *
   * @param filePath The file path to check
   * @return true if the path exists, is a file, and can be read from
   */
  public static boolean fileExistsAndCanRead(final String filePath) {
    return fileExistsAndCanRead(new File(filePath));
  }

  /**
   * Shortcut method to determine if a file object can actually be "read" as a file
   *
   * @param file The file to check
   * @return true if the path exists, is a file, and can be read from
   */
  public static boolean fileExistsAndCanRead(final File file) {
    return file != null && file.exists() && file.isFile() && file.canRead();
  }

  // Various shortcut logging functions
  private static void log(final Level level, final String message, final String... args) {
    LOG.log(level, message, args);
  }

  public static void info(final String message, final String... args) {
    log(Level.INFO, message, args);
  }

  public static void info(final String message) {
    info(message, "");
  }

  public static void warn(final String message, final String... args) {
    log(Level.WARNING, message, args);
  }

  public static void warn(final String message) {warn(message, "");}

  public static void error(final String message, final String... args) {
    log(Level.SEVERE, message, args);
  }

  public static void error(final String message) {
    error(message, "");
  }

  /**
   * Returns a hostname from the current host
   * <p>
   * TODO: Platform dependent
   *
   * @return Either the configured host name, or the hostname of the local IP
   */
  public static String getHostName() {
    return System.getenv().containsKey("HOSTNAME") ?
      Iterables.getFirst(DOT_SPLITTER.split(System.getenv("HOSTNAME")), getInetHost()) :
      getInetHost();
  }

  private static String getInetHost() {
    try {
      return Iterables.getFirst(DOT_SPLITTER.split(InetAddress.getLocalHost().getHostName()), "UNKNOWN_HOST");
    } catch (Exception e) {
      return "UNKNOWN_HOST";
    }
  }

  /**
   * Determines if omicron is running as a root user
   * <p>
   * TODO: Platform dependent
   *
   * @return true if the current user.name is root
   */
  public static boolean isRunningAsRoot() {
    return "root".equals(System.getProperty("user.name"));
  }

  /**
   * Convenience function to get either the lastModified timestamp from a
   * specified file
   *
   * @param filePath The file path to get the timestamp from
   * @return The file lastModified timestamp, or DEFAULT_TIMESTAMP if the file cannot be read
   */
  public static long getTimestampFromPath(final String filePath) {
    if (Strings.isNullOrEmpty(filePath.trim())) {
      return DEFAULT_TIMESTAMP;
    }

    final File configFile = new File(filePath);

    if (!Utils.fileExistsAndCanRead(configFile)) {
      return DEFAULT_TIMESTAMP;
    }

    return configFile.lastModified();
  }


  public static boolean isNullOrEmpty(final String value){
    return value == null || value.trim().isEmpty();
  }

}
