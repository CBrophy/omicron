package com.zulily.omicron;

import com.google.common.base.Splitter;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Utils {
  private final static Logger LOG = Logger.getGlobal();
  public final static Splitter CSV_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
  public final static Splitter EQUAL_SPLITTER = Splitter.on('=').trimResults().omitEmptyStrings();


  public static boolean fileExistsAndCanRead(final File file) {
    return file != null && file.exists() && file.isFile() && file.canRead();
  }

  public static void log(final Level level, final String message, final String... args) {
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

}
